package scalabpe.plugin.cache

import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.buffer.ChannelBuffer

import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.SelfCheckResult

class RedisSoc4Cluster(
        val addrs: String,
        val receiver_f: (Any) => Unit,
        val connectTimeout: Int = 15000,
        val pingInterval: Int = 60000,
        val timerInterval: Int = 100,
        val reconnectInterval: Int = 1) extends Logging with Dumpable with RedisSocTrait {

    var nettyClient: RedisNettyClient4Cluster = _
    var timer: Timer = _
    var slotInited = new AtomicBoolean(false)
    val generator = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[Int, CacheData]()

    var changeSlotPool: ThreadPoolExecutor = _

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

        nettyClient = new RedisNettyClient4Cluster(this,
            addrs,
            connectTimeout,
            pingInterval,
            timerInterval,
            reconnectInterval)

        timer = new Timer("redissoctimer")

        changeSlotPool = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
        changeSlotPool.prestartAllCoreThreads()

        timer.schedule(new TimerTask() {
            def run() {
                sendClusterSlots()
            }
        }, 50, 5000)

        // 等待slot初始化
        val now = System.currentTimeMillis
        var t = 0L
        while (!slotInited.get() && (t - now) < 2000) {
            Thread.sleep(50)
            t = System.currentTimeMillis
        }

        log.info("redissoc4cluster {} started", addrs)
    }

    def close() {
        timer.cancel()
        nettyClient.close()
        changeSlotPool.shutdown()
        log.info("redis soc {} stopped", addrs)
    }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def generatePing(): Tuple2[Int, ChannelBuffer] = {
        val sequence = generateSequence()
        val (ok, buf) = RedisCodec.encode(RedisCodec.MSGID_PING, HashMapStringAny())
        (sequence, buf)
    }

    def generateAsking(): Tuple2[Int, ChannelBuffer] = {
        val sequence = generateSequence()
        val (ok, buf) = RedisCodec.encode(RedisCodec.MSGID_ASKING, HashMapStringAny())
        (sequence, buf)
    }

    def sendClusterSlots() {
        if (slotInited.get()) return
        val addr = nettyClient.getFirstConn()
        if (addr == null) return

        val sequence = generateSequence()
        val (ok, buf) = RedisCodec.encode(RedisCodec.MSGID_CLUSTER_SLOTS, HashMapStringAny())
        val req = new Request("REDISCLUSTERSLOTS_" + sequence, "0:0", 0, 0, 1, RedisCodec.MSGID_CLUSTER_SLOTS, HashMapStringAny(), HashMapStringAny(), null) // 模拟一个request
        val timeout = 3000
        dataMap.put(sequence, new CacheData(req, timeout, 1))
        nettyClient.sendByAddr(sequence, buf, timeout, addr)
    }

    def send(req: Request, timeout: Int): Unit = {
        send(req, timeout, 1)
    }

    def send(req: Request, timeout: Int, times: Int): Unit = {
        if (times >= 3) {
            val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        val (ok1, buf) = RedisCodec.encode(req.msgId, req.body)
        if (!ok1) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        val sequence = generateSequence()
        dataMap.put(sequence, new CacheData(req, timeout, times))

        val slot = getSlot(req)
        var ok2 = nettyClient.sendBySlot(sequence, buf, timeout, slot)
        if (!ok2) {
            val connId = "0.0.0.0:0"
            processError(sequence, connId, ResultCodes.SOC_NOCONNECTION)
        }
    }

    def sendAsking(req: Request, timeout: Int, times: Int, addr: String): Unit = {
        if (times >= 3) {
            val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        val (ok1, buf) = RedisCodec.encode(req.msgId, req.body)
        if (!ok1) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        val sequence = generateSequence()
        dataMap.put(sequence, new CacheData(req, timeout, times))
        var ok2 = nettyClient.sendByAddr(sequence, buf, timeout, addr, asking = true)
        if (!ok2) {
            val connId = "0.0.0.0:0"
            processError(sequence, connId, ResultCodes.SOC_NOCONNECTION)
        }
    }

    def getSlot(req: Request): Int = {
        val key = getKey(req)
        return Crc16.getCRC16(key) & (16384 - 1)
    }

    def getKey(req: Request): String = {
        var key = req.s("key", "")
        if (key == "") {
            var keys = req.ls("keys")
            if (keys != null && keys.size > 0) key = keys(0)
        }
        if (key == "") {
            var source = req.s("source", "")
            if (source != "") key = source
        }
        val p1 = key.indexOf("{")
        if (p1 < 0) return key
        val p2 = key.indexOf("}", p1 + 1)
        if (p2 < 0 || p2 == p1 + 1) return key // ignore {}
        key.substring(p1 + 1, p2)
    }

    def receive(sequence: Int, buf: ChannelBuffer, connId: String): Unit = {
        val saved = dataMap.remove(sequence)
        if (saved == null) {
            return
        }

        val req = saved.data

        if (req.requestId.startsWith("REDISCLUSTERSLOTS_")) {

            val (errorCode, body) = RedisCodec.decode(req.msgId, buf, req)
            if (errorCode != 0) return
            try {
                changeSlotPool.execute(new Runnable() {
                    def run() {
                        val cnt = body.i("slots")
                        for (i <- 0 until cnt) {
                            val min = body.i("slot-" + i + "-min")
                            val max = body.i("slot-" + i + "-max")
                            val addr = body.s("slot-" + i + "-addr")
                            nettyClient.changeSlotAddr(min, max, addr)
                        }
                        slotInited.set(true)
                    }
                })
            } catch {
                case e: Throwable =>
                    log.error("changeSlotPool is full")
            }
            return
        }

        val (errorCode, body) = RedisCodec.decode(req.msgId, buf, req)
        if (errorCode == RedisCodec.MOVED_ERROR) {
            val slot = body.i("slot")
            val addr = body.ns("addr")
            //println("moved to slot="+slot+",key="+req.ns("key")+",connId="+connId+",new addr="+addr)        
            try {
                changeSlotPool.execute(new Runnable() {
                    def run() {
                        nettyClient.changeSlotAddr(slot, addr) // 修改地址
                        send(req, saved.timeout, saved.times + 1) // 重新发送
                        slotInited.set(false)
                    }
                })
            } catch {
                case e: Throwable =>
                    log.error("changeSlotPool is full")
            }
            return
        }
        if (errorCode == RedisCodec.ASK_ERROR) {
            val slot = body.i("slot")
            val addr = body.ns("addr")
            sendAsking(req, saved.timeout, saved.times + 1, addr) // 重新发送ASKING
            return
        }
        val res = new Response(errorCode, body, req)
        res.remoteAddr = parseRemoteAddr(connId)
        receiver_f(new RequestResponseInfo(req, res))
        postReceive(req)
    }

    def postReceive(req: Request) {
        val msgId = req.msgId
        if (RedisCodec.isWriteOp(msgId) && !RedisCodec.isSetOp(msgId)) {
            val expire = req.i("expire")
            if (expire > 0) {
                val (ok, buf) = RedisCodec.encode(RedisCodec.MSGID_EXPIRE, req.body)
                val timeout = 3000
                val sequence = generateSequence()
                val slot = getSlot(req)
                nettyClient.sendBySlot(sequence, buf, timeout, slot)
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

        val req = saved.data
        if (req.requestId.startsWith("REDISCLUSTERSLOTS_")) { // soc自己发出的指令
            return
        }

        val res = createErrorResponse(errorCode, req)
        res.remoteAddr = parseRemoteAddr(connId)
        receiver_f(new RequestResponseInfo(req, res))
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

    class CacheData(val data: Request, val timeout: Int, val times: Int)
}

