package scalabpe.plugin.cache

import java.util.Timer
import java.util.TimerTask
import java.util.TreeMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

import MemCacheSoc.TYPE_ARRAYHASH
import MemCacheSoc.TYPE_CONHASH
import MemCacheSoc.TYPE_MASTERSLAVE
import scalabpe.core.ArrayBufferMap
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.SelfCheckResult

object MemCacheSoc {
    val TYPE_UNKNOWN = -1
    val TYPE_ARRAYHASH = 1
    val TYPE_CONHASH = 2
    val TYPE_MASTERSLAVE = 3
}

class MemCacheSoc(
        val addrs: String,
        val cacheType: Int,
        val receiver_f: (Any) => Unit,
        val connectTimeout: Int = 15000,
        val pingInterval: Int = 60000,
        val connSizePerAddr: Int = 4,
        val timerInterval: Int = 100,
        val reconnectInterval: Int = 1,
        val failOver: Boolean = true,
        val maxErrorCount: Int = 50) extends Logging with Dumpable {

    val addrArray = addrs.split(",")
    val addrSize = addrArray.size

    val addrValid = new Array[AtomicBoolean](addrSize)
    val addrErrCnt = new Array[AtomicInteger](addrSize)
    var timer: Timer = _

    var treeMap: TreeMap[Long, Int] = _

    var nettyClient: MemCacheNettyClient = _
    val generator = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[Int, CacheData]()

    init

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {
        val buff = nettyClient.selfcheck()
        buff
    }

    def dump() {

        val buff = new StringBuilder
        buff.append("dataMap.size=").append(dataMap.size).append(",")
        log.info(buff.toString)

        nettyClient.dump
    }

    def init() {

        for (i <- 0 until addrSize) addrValid(i) = new AtomicBoolean(true)
        for (i <- 0 until addrSize) addrErrCnt(i) = new AtomicInteger(0)
        if (cacheType == TYPE_CONHASH) initTreeMap()

        timer = new Timer("memcachesoctimer")

        timer.schedule(new TimerTask() {
            def run() {
                testOfflineAddrs()
            }
        }, 5000, 1000)

        nettyClient = new MemCacheNettyClient(this,
            addrs,
            connectTimeout,
            pingInterval,
            connSizePerAddr,
            timerInterval,
            reconnectInterval)
        log.info("memcache soc {} started", addrs)
    }

    def initTreeMap() {
        treeMap = new TreeMap[Long, Int]()
        for (i <- 0 until addrSize) {
            for (j <- 0 until 50) {
                val h = MurmurHash.hash(addrArray(i) + "-" + i + "-" + j)
                treeMap.put(h, i)
            }
        }
    }

    def close() {
        timer.cancel()
        nettyClient.close()
        log.info("memcache soc {} stopped", addrs)
    }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def selectAddrIdx(key: String, times: Int): Int = {
        cacheType match {
            case TYPE_ARRAYHASH =>
                val h = MurmurHash.hash(key)
                val d = h % addrSize
                var idx = if (d < 0) (-1 * d).toInt else d.toInt
                if (failOver) nextValid(idx)
                else idx
            case TYPE_CONHASH =>
                val h = MurmurHash.hash(key)
                val sortedMap = treeMap.tailMap(h)
                var idx = 0
                if (sortedMap == null || sortedMap.isEmpty)
                    idx = treeMap.get(treeMap.firstKey).toInt
                else
                    idx = treeMap.get(sortedMap.firstKey).toInt
                if (failOver) nextValid(idx)
                else idx
            case TYPE_MASTERSLAVE =>
                var idx = if (times == 1) 0 else 1
                if (failOver && idx == 0) nextValid(idx)
                else idx
        }
    }

    def nextValid(idx: Int): Int = {
        var i = idx
        if (addrValid(i).get()) return i
        i += 1
        if (i >= addrSize) i = 0
        while (!addrValid(i).get() && i != idx) {
            i += 1
            if (i >= addrSize) i = 0
        }
        i
    }

    def send(req: Request, timeout: Int, toAddr: Int = -1, times: Int = 1): Unit = {

        val keys = req.ls("keys")
        if (req.msgId != MemCacheCodec.MSGID_MGET || keys == null || keys.size == 0) {
            sendToSingleAddr(req, timeout, toAddr, times)
            return
        }

        // 特殊逻辑：每个消息单独发get,最后合并在一起

        var i = 0

        // save the parent request
        val sequence = generateSequence()
        val data = new CacheData(req, timeout, times, -1)
        data.responses = new Array[Response](keys.size)
        dataMap.put(sequence, data)

        for (key <- keys) {
            val subreqid = "sub:" + req.requestId + ":" + i
            val body = HashMapStringAny("key" -> key, "parentSequence" -> sequence)
            val subreq = new Request(
                subreqid, "dummy", 0, 0, req.serviceId, MemCacheCodec.MSGID_GET, null,
                body, null)
            sendToSingleAddr(subreq, timeout, toAddr, times)
            i += 1
        }

    }

    def sendToSingleAddr(req: Request, timeout: Int, toAddr: Int = -1, times: Int = 1): Unit = {

        val sequence = generateSequence()
        val (ok1, buf) = MemCacheCodec.encode(sequence, req.msgId, req.body)
        if (!ok1) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        val key = req.s("key", "")
        val addrIdx = if (toAddr == -1) selectAddrIdx(key, times) else toAddr
        //println("cacheType=%d,key=%s,addr=%d".format(cacheType,key,addrIdx))
        dataMap.put(sequence, new CacheData(req, timeout, times, addrIdx))

        var ok2 = nettyClient.sendByAddr(sequence, buf, timeout, addrIdx)
        if (!ok2) {
            val connId = addrArray(addrIdx) + ":0"
            processError(sequence, connId, ResultCodes.SOC_NOCONNECTION)
        }
    }

    def testOfflineAddrs() {

        val timeout = 3000
        var buf: ChannelBuffer = null

        for (i <- 0 until addrSize if !addrValid(i).get()) {
            val sequence = generateSequence()
            if (buf == null) {
                val (ok, buf2) = MemCacheCodec.encode(sequence, MemCacheCodec.MSGID_NOOP, HashMapStringAny())
                buf = buf2
            }
            dataMap.put(sequence, new CacheData(null, timeout, 1, i, true)) // req is null
            var ok2 = nettyClient.sendByAddr(sequence, buf, timeout, i)
            if (!ok2) {
                val connId = addrArray(i) + ":0"
                processError(sequence, connId, ResultCodes.SOC_NOCONNECTION)
            }
        }

    }

    def receive(sequence: Int, buf: ChannelBuffer, connId: String): Unit = {
        val saved = dataMap.remove(sequence)
        if (saved == null) {
            return
        }

        addrOk(saved.addrIdx)
        if (saved.testOnly) return

        val req = saved.data
        if (saved.times == 1) { // for hash or master
            val (errorCode, body) = MemCacheCodec.decode(req.msgId, buf, req)
            val res = new Response(errorCode, body, req)
            res.remoteAddr = parseRemoteAddr(connId)
            if (req.requestId.startsWith("sub:")) { // mget 子请求
                processMgetResponse(req, res)
                return
            }
            receiver_f(new RequestResponseInfo(req, res))
            postReceive(req, saved.addrIdx)

            if (cacheType == TYPE_MASTERSLAVE && MemCacheCodec.isWriteOp(req.msgId)) {
                val addr = if (saved.addrIdx == 0) 1 else 0
                send(req, saved.timeout, addr, 2) // write to slave(depends the addr first read), ignore the response(times=2)
            }
        }
        if (saved.times == 2) { // only for slave
            postReceive(req, saved.addrIdx)
        }
    }

    def postReceive(req: Request, addrIdx: Int) {}

    def networkError(sequence: Int, connId: String) {
        processError(sequence, connId, ResultCodes.SOC_NETWORKERROR)
    }
    def timeoutError(sequence: Int, connId: String) {
        processError(sequence, connId, ResultCodes.SOC_TIMEOUT)
    }

    def processError(sequence: Int, connId: String, errorCode: Int) {
        val saved = dataMap.remove(sequence)
        if (saved == null) {
            return
        }

        if (errorCode != ResultCodes.SOC_TIMEOUT)
            addrFailed(saved.addrIdx)

        if (saved.testOnly) return

        val req = saved.data
        if (saved.times == 1) { // for hash or master

            if (cacheType == TYPE_MASTERSLAVE && MemCacheCodec.isReadOp(req.msgId) && saved.addrIdx == 0) {
                send(req, saved.timeout, 1, 1) // read from slave(toAddr=1), need the response (times=1)
                return
            }

            val res = createErrorResponse(errorCode, req)
            res.remoteAddr = parseRemoteAddr(connId)
            if (req.requestId.startsWith("sub:")) { // mget 子请求
                processMgetResponse(req, res)
                return
            }
            receiver_f(new RequestResponseInfo(req, res))
        }

    }

    def processMgetResponse(subreq: Request, subRes: Response) {
        val p = subreq.requestId.lastIndexOf(":")
        if (p < 0) return
        val idx = subreq.requestId.substring(p + 1).toInt
        val parentSequence = subreq.i("parentSequence")
        val parentData = dataMap.get(parentSequence)
        if (parentData == null) return
        if (idx < 0 || idx >= parentData.responses.size) return
        subRes.body.put("key", subreq.s("key"))
        parentData.responses(idx) = subRes
        val finished = parentData.responses.forall(_ != null)
        if (!finished) return
        dataMap.remove(parentSequence)
        val errorResponse = parentData.responses.filter(r => r.code < 0 && r.code != ResultCodes.CACHE_NOT_FOUND)
        val req = parentData.data
        if (errorResponse.size > 0) {
            val first = errorResponse(0)
            val res = new Response(first.code, HashMapStringAny(), req)
            res.remoteAddr = first.remoteAddr
            receiver_f(new RequestResponseInfo(req, res))
            return
        }
        val keyvalues = ArrayBufferMap()
        for (res <- parentData.responses) {
            keyvalues += HashMapStringAny("key" -> res.body.s("key"), "value" -> res.body.s("value"))
        }

        val res = new Response(0, HashMapStringAny("keyvalues" -> keyvalues), req)
        res.remoteAddr = parentData.responses(0).remoteAddr
        receiver_f(new RequestResponseInfo(req, res))
    }

    def addrOk(addrIdx: Int) {
        if (!failOver) return
        var cnt = addrErrCnt(addrIdx).intValue()
        if (cnt >= 1) cnt = addrErrCnt(addrIdx).decrementAndGet()
        if (cnt <= 0 && !addrValid(addrIdx).get()) {
            addrValid(addrIdx).set(true)
            log.warn("set addr to online, addr=" + addrArray(addrIdx))
        }
    }

    def addrFailed(addrIdx: Int) {
        if (!failOver) return
        var cnt = addrErrCnt(addrIdx).intValue()
        if (cnt < maxErrorCount) cnt = addrErrCnt(addrIdx).incrementAndGet()
        if (cnt >= maxErrorCount && addrValid(addrIdx).get()) {
            addrValid(addrIdx).set(false)
            log.warn("set addr to offline, addr=" + addrArray(addrIdx))
        }
    }

    def generatePing(): Tuple2[Int, ChannelBuffer] = {
        val sequence = generateSequence()
        val (ok, buf) = MemCacheCodec.encode(sequence, MemCacheCodec.MSGID_NOOP, HashMapStringAny())
        (sequence, buf)
    }

    def createErrorResponse(code: Int, req: Request): Response = {
        val res = new Response(code, new HashMapStringAny(), req)
        res
    }

    def parseRemoteAddr(connId: String): String = {
        val p = connId.lastIndexOf(":")

        if (p >= 0)
            connId.substring(0, p)
        else
            "0.0.0.0:0"
    }

    class CacheData(val data: Request, val timeout: Int, val times: Int, val addrIdx: Int, val testOnly: Boolean = false) {
        var responses: Array[Response] = null
    }

    class MemCacheRequest(val opcode: Int) {
        var key_bs: ChannelBuffer = null
        var value_bs: ChannelBuffer = null
        var extra_bs: ChannelBuffer = null
        var cas = 0L

        def this(opcode: Int, key: String) {
            this(opcode)
            val bs = key.getBytes("utf-8")
            key_bs = ChannelBuffers.wrappedBuffer(bs)
        }
        def setValue(value: String) {
            val bs = value.getBytes("utf-8")
            value_bs = ChannelBuffers.wrappedBuffer(bs)
        }

        /*
        memcache内部： 0认为用不过期；30天内认为是相对时间；超过30天，认为是是UNIX时间戳，而不是自当前时间开始的秒数偏移值
        实现：
        exp <= 0 永不过期
        exp <= 60*60*24*30（30天的秒数）, 则过期时间为当前时间 + 设定的秒数
        exp >= 当前时间, 认为传入的就是过期时间
        否则，用当前时间+对应秒数
         */
        def adjust(exp: Int): Int = {
            if (exp <= 0) return 0 // never expire
            if (exp <= 30 * 24 * 60 * 60) return exp
            val now = (System.currentTimeMillis() / 1000).toInt
            if (exp > now) return exp // an absolute date
            now + exp // generate date by exp
        }

        def setExtra(flags: Int, expire: Int) {
            extra_bs = ChannelBuffers.buffer(8)
            extra_bs.writeInt(flags)
            extra_bs.writeInt(adjust(expire))
        }
        def setExtra(step: Long, init: Long, expire: Int) {
            extra_bs = ChannelBuffers.buffer(20)
            extra_bs.writeLong(step)
            extra_bs.writeLong(init)
            extra_bs.writeInt(adjust(expire))
        }
    }

    object MemCacheCodec {

        val MSGID_GET = 1
        val MSGID_SET = 2
        val MSGID_ADD = 3
        val MSGID_REPLACE = 4
        val MSGID_DELETE = 5
        val MSGID_INCREMENT = 6
        val MSGID_DECREMENT = 7
        val MSGID_MGET = 8
        val MSGID_NOOP = 100

        val magicReq = 0x80.toByte
        val magicRes = 0x81.toByte

        val opcodeMap = HashMap[Int, Int](
            MSGID_NOOP -> 0x0a,
            MSGID_GET -> 0x00,
            MSGID_SET -> 0x01,
            MSGID_ADD -> 0x02,
            MSGID_REPLACE -> 0x03,
            MSGID_DELETE -> 0x04,
            MSGID_INCREMENT -> 0x05,
            MSGID_DECREMENT -> 0x06)

        def isWriteOp(msgId: Int): Boolean = {
            !isReadOp(msgId)
        }

        def isReadOp(msgId: Int): Boolean = {
            msgId match {
                case MSGID_GET | MSGID_MGET =>
                    true
                case _ =>
                    false
            }
        }

        def encode(sequence: Int, msgId: Int, map: HashMapStringAny): Tuple2[Boolean, ChannelBuffer] = {

            val opcode = opcodeMap.getOrElse(msgId, -1)
            if (opcode == -1) return (false, null)

            var b: ChannelBuffer = null
            if (msgId == MSGID_NOOP) {
                val req = new MemCacheRequest(opcode)
                b = encode(sequence, req)
                return (true, b)
            }

            val key = map.s("key", "")
            if (key == "") {
                return (false, null)
            }

            msgId match {
                case MSGID_GET =>
                    val req = new MemCacheRequest(opcode, key)
                    var cas = map.s("cas", "0")
                    if (cas == "") cas = "0"
                    req.cas = cas.toLong
                    b = encode(sequence, req)
                case MSGID_DELETE =>
                    val req = new MemCacheRequest(opcode, key)
                    b = encode(sequence, req)
                case MSGID_SET | MSGID_ADD | MSGID_REPLACE =>
                    val req = new MemCacheRequest(opcode, key)
                    val value = map.s("value", "")
                    req.setValue(value)
                    val flags = map.i("flags")
                    val expire = map.i("expire")
                    var cas = map.s("cas", "0")
                    if (cas == "") cas = "0"
                    req.cas = cas.toLong
                    req.setExtra(flags, expire)
                    b = encode(sequence, req)
                case MSGID_INCREMENT | MSGID_DECREMENT =>
                    val req = new MemCacheRequest(opcode, key)
                    val step = map.s("step", "1").toLong
                    val init = map.s("init", "0").toLong
                    val expire = map.i("expire")
                    req.setExtra(step, init, expire)
                    b = encode(sequence, req)
                case _ =>
            }

            (b != null, b)
        }

        def encode(sequence: Int, req: MemCacheRequest): ChannelBuffer = {

            val d = ChannelBuffers.dynamicBuffer()

            val extra_length = if (req.extra_bs != null) req.extra_bs.readableBytes else 0
            val key_length = if (req.key_bs != null) req.key_bs.readableBytes else 0
            val value_length = if (req.value_bs != null) req.value_bs.readableBytes else 0
            val total_length = extra_length + key_length + value_length

            d.writeByte(magicReq)
            d.writeByte(req.opcode)
            d.writeShort(key_length) // 
            d.writeByte(extra_length) // extra length 
            d.writeByte(0) // data type
            d.writeShort(0) // status
            d.writeInt(total_length) // total length
            d.writeInt(sequence) // opaque: sequence
            d.writeLong(req.cas) // cas

            if (extra_length > 0) {
                d.writeBytes(req.extra_bs)
            }
            if (key_length > 0) {
                d.writeBytes(req.key_bs)
            }
            if (value_length > 0) {
                d.writeBytes(req.value_bs)
            }

            return d
        }

        def decode(msgId: Int, buf: ChannelBuffer, req: Request): Tuple2[Int, HashMapStringAny] = {
            val magic = buf.readByte()
            if (magic != magicRes) return (ResultCodes.TLV_DECODE_ERROR, null)
            val opcode = buf.readByte()
            val reqopcode = opcodeMap.getOrElse(msgId, -1).toByte
            if (opcode != reqopcode) return (ResultCodes.TLV_DECODE_ERROR, null)

            val keyLength = buf.readShort()
            val extraLength = buf.readByte()
            buf.readByte() // ignore data type
            val status = buf.readShort()
            val totalLength = buf.readInt()
            val sequence = buf.readInt() // opaque

            val cas = buf.readLong()
            var flags = 0
            if (extraLength > 0) {
                val bs = buf.readBytes(extraLength) // ignore
                if (opcode == 0x00) flags = bs.readInt() // flags
            }
            var key: String = null
            if (keyLength > 0) {
                val bs = new Array[Byte](keyLength)
                buf.readBytes(bs)
                key = new String(bs, "utf-8")
            }
            var value: String = null
            val valueLength = totalLength - keyLength - extraLength
            if (valueLength > 0) {
                if (msgId == MSGID_INCREMENT || msgId == MSGID_DECREMENT) {
                    value = buf.readLong().toString
                } else {
                    val bs = new Array[Byte](valueLength)
                    buf.readBytes(bs)
                    value = new String(bs, "utf-8")
                }
            }

            /*
            0x0000  No error
            0x0001  Key not found
            0x0002  Key exists
            0x0003  Value too large
            0x0004  Invalid arguments
            0x0005  Item not stored
            0x0006  Incr/Decr on non-numeric value. 
             */
            val errorCode = status match {
                case 0x0000 => 0
                case 0x0001 => ResultCodes.CACHE_NOT_FOUND
                case _      => ResultCodes.CACHE_UPDATEFAILED
            }
            val map = HashMapStringAny()
            if (errorCode == 0) {
                if (key != null) {
                    map.put("key", key)
                }
                if (value != null) {
                    map.put("value", value)
                }
                map.put("cas", cas.toString)
                map.put("flags", flags)
            }
            (errorCode, map)
        }

    }

}

