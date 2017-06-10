package scalabpe.plugin.cache

import java.util.Timer
import java.util.TimerTask
import java.util.TreeMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.buffer.ChannelBuffer

import RedisType.TYPE_ARRAYHASH
import RedisType.TYPE_CONHASH
import RedisType.TYPE_MASTERSLAVE
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.SelfCheckResult

class RedisSoc(
        val addrs: String,
        val cacheType: Int,
        val receiver_f: (Any) => Unit,
        val connectTimeout: Int = 15000,
        val pingInterval: Int = 60000,
        val connSizePerAddr: Int = 4,
        val timerInterval: Int = 100,
        val reconnectInterval: Int = 1,
        val failOver: Boolean = true,
        val maxErrorCount: Int = 50) extends Logging with Dumpable with RedisSocTrait {

    val addrArray = addrs.split(",")
    val addrSize = addrArray.size

    val addrValid = new Array[AtomicBoolean](addrSize)
    val addrErrCnt = new Array[AtomicInteger](addrSize)
    var timer: Timer = _

    var treeMap: TreeMap[Long, Int] = _

    var nettyClient: RedisNettyClient = _
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

        timer = new Timer("redissoctimer")

        timer.schedule(new TimerTask() {
            def run() {
                testOfflineAddrs()
            }
        }, 5000, 1000)

        nettyClient = new RedisNettyClient(this,
            addrs,
            connectTimeout,
            pingInterval,
            connSizePerAddr,
            timerInterval,
            reconnectInterval)
        log.info("redis soc {} started", addrs)
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
        log.info("redis soc {} stopped", addrs)
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

    def send(req: Request, timeout: Int): Unit = {
        send(req, timeout, -1, 1)
    }

    def send(req: Request, timeout: Int, toAddr: Int = -1, times: Int = 1): Unit = {

        val keys = req.ls("keys")
        if (keys == null || keys.size < 2 || cacheType == TYPE_MASTERSLAVE || addrSize == 1) {
            sendToSingleAddr(req, timeout, toAddr, times)
            return
        }

        // 传入keys参数，key可能分布在不同的redis实例下，此插件不处理这种情况
        val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
        receiver_f(new RequestResponseInfo(req, res))
    }

    def sendToSingleAddr(req: Request, timeout: Int, toAddr: Int = -1, times: Int = 1): Unit = {

        val (ok1, buf) = RedisCodec.encode(req.msgId, req.body)
        if (!ok1) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        val sequence = generateSequence()

        var key = req.s("key", "")
        if (key == "") {
            var keys = req.ls("keys")
            if (keys != null && keys.size > 0) key = keys(0)
        }
        if (key == "") {
            var source = req.s("source", "")
            if (source != "") key = source
        }
        val addrIdx = if (toAddr == -1) selectAddrIdx(key, times) else toAddr
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
            if (buf == null) {
                val (ok, buf2) = RedisCodec.encode(RedisCodec.MSGID_PING, HashMapStringAny())
                buf = buf2
            }
            val sequence = generateSequence()
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
            val (errorCode, body) = RedisCodec.decode(req.msgId, buf, req)
            val res = new Response(errorCode, body, req)
            res.remoteAddr = parseRemoteAddr(connId)
            receiver_f(new RequestResponseInfo(req, res))
            postReceive(req, saved.addrIdx)

            if (cacheType == TYPE_MASTERSLAVE && RedisCodec.isWriteOp(req.msgId)) {
                val addr = if (saved.addrIdx == 0) 1 else 0
                send(req, saved.timeout, addr, 2) // write to slave(depends the addr first read), ignore the response(times=2)
            }
        }
        if (saved.times == 2) { // only for slave
            postReceive(req, saved.addrIdx)
        }
    }

    def postReceive(req: Request, addrIdx: Int) {
        val msgId = req.msgId
        if (RedisCodec.isWriteOp(msgId) && !RedisCodec.isSetOp(msgId)) {
            val expire = req.i("expire")
            if (expire > 0) {
                val (ok, buf) = RedisCodec.encode(RedisCodec.MSGID_EXPIRE, req.body)
                val timeout = 3000
                val sequence = generateSequence()
                nettyClient.sendByAddr(sequence, buf, timeout, addrIdx)
            }
        }
    }

    def networkError(sequence: Int, connId: String) {
        processError(sequence, connId, ResultCodes.SOC_NOCONNECTION)
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

            if (cacheType == TYPE_MASTERSLAVE && RedisCodec.isReadOp(req.msgId) && saved.addrIdx == 0) {
                send(req, saved.timeout, 1, 1) // read from slave(toAddr=1), need the response (times=1)
                return
            }

            val res = createErrorResponse(errorCode, req)
            res.remoteAddr = parseRemoteAddr(connId)
            receiver_f(new RequestResponseInfo(req, res))
        }

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
        val (ok, buf) = RedisCodec.encode(RedisCodec.MSGID_PING, HashMapStringAny())
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

    class CacheData(val data: Request, val timeout: Int, val times: Int, val addrIdx: Int, val testOnly: Boolean = false)

}

