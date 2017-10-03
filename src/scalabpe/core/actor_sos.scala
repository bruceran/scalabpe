package scalabpe.core

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
class Sos(val router: Router, val port: Int) extends Actor with RawRequestActor with Sos4Netty with Logging with Dumpable {

    val EMPTY_BUFFER = ChannelBuffers.buffer(0)
    var nettyServer: NettyServer = _

    val converter = new AvenueCodec

    var threadNum = 2
    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    var timeout = 30000
    var idleTimeoutMillis: Int = 180000
    var maxPackageSize: Int = 2000000
    var maxConns = 500000
    var host: String = "*"
    var timerInterval: Int = 100

    val dataMap = new ConcurrentHashMap[Int, CacheData]()
    val generator = new AtomicInteger(1)
    var codecs: TlvCodecs = _
    var qte: QuickTimerEngine = null

    var reverseServiceIds = "0"
    var hasReverseServiceIds = false
    var spsDisconnectNotifyTo = "55605:111"
    var reverseIps: HashSet[String] = null

    var pushToIpPort: Boolean = false
    val connsAddrMap = new HashMap[String, String]()
    val connsLock = new ReentrantLock(false)

    var isSps = false
    var isEncrypted = false
    var shakeHandsServiceIdMsgId = "1:5"
    var aesKeyMap = new ConcurrentHashMap[String, String]()

    init

    def init() {

        codecs = router.codecs

        val cfgNode = router.cfgXml \ "ServerSos"
        if (cfgNode != null) {

            var s = (cfgNode \ "@threadNum").text
            if (s != "") threadNum = s.toInt

            s = (cfgNode \ "@host").text
            if (s != "") host = s

            s = (cfgNode \ "@timeout").text
            if (s != "") timeout = s.toInt

            s = (cfgNode \ "@timerInterval").text
            if (s != "") timerInterval = s.toInt

            s = (cfgNode \ "@maxPackageSize").text
            if (s != "") maxPackageSize = s.toInt

            s = (cfgNode \ "@maxConns").text
            if (s != "") maxConns = s.toInt

            s = (cfgNode \ "@idleTimeoutMillis").text
            if (s != "") idleTimeoutMillis = s.toInt

            s = (cfgNode \ "@isEncrypted").text
            if (s != "") isEncrypted = TypeSafe.isTrue(s)

            s = (cfgNode \ "@shakeHandsServiceIdMsgId").text
            if (s != "") shakeHandsServiceIdMsgId = s

            s = (cfgNode \ "@reverseServiceIds").text
            if (s != "") {
                reverseServiceIds = s
            }
            hasReverseServiceIds = (reverseServiceIds != "0")
            router.parameters.put("reverseServiceIds", reverseServiceIds)

            s = (cfgNode \ "@spsReportTo").text
            if (s != "") {
                router.parameters.put("spsReportTo", s)
            }

            s = (cfgNode \ "@spsDisconnectNotifyTo").text
            if (s != "") {
                spsDisconnectNotifyTo = s
            }

            s = (cfgNode \ "@pushToIpPort").text
            if (s != "") pushToIpPort = TypeSafe.isTrue(s)

            s = (cfgNode \ "@isSps").text
            if (s != "") {
                isSps = TypeSafe.isTrue(s)
                router.parameters.put("isSps", if (isSps) "1" else "0")
            }

            if (!isSps) {
                isEncrypted = false
            } else {
                pushToIpPort = true
            }

            val reverseIpList = (cfgNode \ "ReverseIp").map(_.text).toList
            if (reverseIpList.size > 0) {
                reverseIps = new HashSet[String]()
                reverseIpList.foreach(reverseIps.add)
                log.info("reverseIps=" + reverseIps.mkString(","))
            }

        }

        nettyServer = new NettyServer(this,
            port,
            host, idleTimeoutMillis, maxPackageSize, maxConns)

        threadFactory = new NamedThreadFactory("serversos")
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        qte = new QuickTimerEngine(onTimeout, timerInterval)

        log.info("sos started")
    }

    def closeReadChannel() {
        nettyServer.closeReadChannel()
    }

    def close() {

        if (pool != null) {
            val t1 = System.currentTimeMillis
            pool.shutdown()
            pool.awaitTermination(5, TimeUnit.SECONDS)
            val t2 = System.currentTimeMillis
            if (t2 - t1 > 100)
                log.warn("long time to close sos threadpool, ts={}", t2 - t1)
        }

        nettyServer.close()

        if (qte != null) {
            qte.close()
            qte = null
        }

        log.info("sos stopped")
    }

    def start() {
        nettyServer.start()
    }

    def stats(): Array[Int] = {
        nettyServer.stats
    }

    def dump() {
        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")
        buff.append("connsAddrMap.size=").append(connsAddrMap.size).append(",")

        log.info(buff.toString)

        nettyServer.dump

        qte.dump()
    }

    override def connected(connId: String) {

        if (!hasReverseServiceIds) return

        if (!pushToIpPort ) return

        val remoteIp = parseRemoteIp(connId)
        if (reverseIps != null) {
            if (!reverseIps.contains(remoteIp)) return
        }

        connsLock.lock()
        try {

            if (pushToIpPort) {
                connsAddrMap.put(parseRemoteAddr(connId), connId)
            }

        } finally {
            connsLock.unlock()
        }

    }

    override def disconnected(connId: String) {

        if (isEncrypted) {
            aesKeyMap.remove(connId)
        }

        if (isSps)
            notifyDisconnected(connId)

        if (!hasReverseServiceIds) return

        if (!pushToIpPort ) return

        val remoteIp = parseRemoteIp(connId)
        if (reverseIps != null) {
            if (!reverseIps.contains(remoteIp)) return
        }

        connsLock.lock()
        try {

            if (pushToIpPort) {
                connsAddrMap.remove(parseRemoteAddr(connId))
            }

        } finally {
            connsLock.unlock()
        }

    }

    def notifyDisconnected(connId: String) {
        if (spsDisconnectNotifyTo == "0:0") return
        val sequence = generateSequence()
        val notifyInfo = spsDisconnectNotifyTo.split(":")
        val version = router.codecs.version(notifyInfo(0).toInt)
        val data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            version,
            notifyInfo(0).toInt,
            notifyInfo(1).toInt,
            sequence,
            0,
            1,
            0,
            ChannelBuffers.dynamicBuffer(128), EMPTY_BUFFER)

        TlvCodec4Xhead.appendAddr(data.xhead, parseRemoteAddr(connId), isSps, version)
        val requestId = "SOS" + RequestIdGenerator.nextId()
        val rr = new RawRequest(requestId, data, connId, this)
        receive(rr)
    }

    def selectConnId(t: String, socId: String = null): String = {

        var toAddr = t
        if (toAddr == null || toAddr == "" || toAddr == "*") {
            toAddr = socId
        }

        if (toAddr == null || toAddr == "" || toAddr == "*") {
            return null
        }

        val toAddrTokenArray = toAddr.split(":")
        if (toAddrTokenArray.length >= 3) {
            return toAddr // treat as connId
        }

        if (toAddrTokenArray.length == 2 && pushToIpPort) {
            connsLock.lock()
            try {
                val connId = connsAddrMap.getOrElse(toAddr, null) // find connId
                return connId
            } finally {
                connsLock.unlock()
            }
        }
        
        return null
    }

    override def receive(v: Any) {

        try {
            pool.execute(new Runnable() {
                def run() {
                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error("sos receive exception v={}", v, e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("sos queue is full")
        }

    }

    def onReceive(v: Any): Unit = {

        v match {

            case rawReq: RawRequest =>

                if (rawReq.sender eq this)
                    router.send(rawReq)
                else
                    send(rawReq, timeout)

            case reqResInfo: RawRequestResponseInfo =>

                if (reqResInfo.rawReq.sender eq this) {
                    if (!reqResInfo.rawReq.requestId.startsWith("SOS")) // sent by sos itself
                        reply(reqResInfo.rawRes.data, reqResInfo.rawRes.connId)
                } else {
                    router.reply(reqResInfo)
                }

            case rawRes: RawResponse =>

                val serviceIdMsgId = rawRes.data.serviceId + ":" + rawRes.data.msgId
                if (!in(spsDisconnectNotifyTo, serviceIdMsgId)) { // sent by sos itself
                    reply(rawRes.data, rawRes.connId)
                }

            case ackInfo: RawRequestAckInfo =>

                if (ackInfo.rawReq.sender eq this)
                    replyAck(ackInfo.rawReq.data, ackInfo.rawReq.connId)
                else
                    router.receiveAck(ackInfo)

            case req: Request =>

                send(req, timeout)

            case reqResInfo: RequestResponseInfo =>

                router.reply(reqResInfo)

            case reqAckInfo: RequestAckInfo =>

                router.receiveAck(reqAckInfo)

            //internal messages

            case SosSendTimeout(sequence) =>

                val cachedata = dataMap.remove(sequence)
                if (cachedata != null) {
                    cachedata.data match {
                        case d: Request =>
                            val res = createErrorResponse(ResultCodes.SOC_TIMEOUT, d)
                            receive(new RequestResponseInfo(d, res))
                        case d: RawRequest =>
                            val res = createErrorResponse(ResultCodes.SOC_TIMEOUT, d)
                            receive(new RawRequestResponseInfo(d, res))
                    }
                } else {
                    log.error("timeout but sequence not found, seq={}", sequence)
                }

            case SosSendAck(data, connId) =>

                val saved = dataMap.get(data.sequence) // donot remove
                if (saved != null) {
                    saved.data match {
                        case d: Request =>
                            receive(new RequestAckInfo(d))
                        case d: RawRequest =>
                            receive(new RawRequestAckInfo(d))
                    }
                } else {
                    log.warn("receive but sequence not found, seq={}", data.sequence)
                }

            case SosSendResponse(data, connId) =>

                val saved = dataMap.remove(data.sequence)
                if (saved != null) {
                    saved.timer.cancel()
                    saved.data match {
                        case req: Request =>
                            val tlvCodec = codecs.findTlvCodec(req.serviceId)
                            if (tlvCodec != null) {

                                val (body, ec) = tlvCodec.decodeResponse(req.msgId, data.body, data.encoding)
                                var errorCode = data.code
                                if (errorCode == 0 && ec != 0) {
                                    errorCode = ec
                                    log.error("decode response error, serviceId=" + req.serviceId + ", msgId=" + req.msgId)
                                }

                                val res = new Response(errorCode, body, req)
                                res.remoteAddr = parseRemoteAddr(connId)
                                receive(new RequestResponseInfo(req, res))
                            }
                        case req: RawRequest =>
                            val newdata = new AvenueData(AvenueCodec.TYPE_RESPONSE,
                                    req.data.version,
                                req.data.serviceId,
                                req.data.msgId,
                                req.data.sequence,
                                0,
                                req.data.encoding,
                                data.code,
                                data.xhead,
                                data.body)
                            val res = new RawResponse(newdata, req)
                            res.remoteAddr = parseRemoteAddr(connId)
                            receive(new RawRequestResponseInfo(req, res))
                    }
                } else {
                    log.warn("receive but sequence not found, seq={}", data.sequence)
                }

            case _ =>

                log.error("unknown msg received")
        }
    }

    def isAck(code: Int) = { code == AvenueCodec.ACK_CODE }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def createErrorResponse(code: Int, req: RawRequest): RawResponse = {
        val data = new AvenueData(AvenueCodec.TYPE_RESPONSE,
                req.data.version,
            req.data.serviceId,
            req.data.msgId,
            req.data.sequence,
            0,
            req.data.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER)
        val res = new RawResponse(data, req.connId)
        res
    }

    def createErrorResponse(code: Int, req: Request): Response = {
        val res = new Response(code, new HashMapStringAny(), req)
        res
    }

    def send(req: Request, timeout: Int): Unit = {
        val tlvCodec = codecs.findTlvCodec(req.serviceId)
        if (tlvCodec == null) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            router.reply(new RequestResponseInfo(req, res))
            return
        }

        val sequence = generateSequence()

        val xhead = TlvCodec4Xhead.encode(req.serviceId, req.xhead, tlvCodec.version)
        val (body, ec) = tlvCodec.encodeRequest(req.msgId, req.body, req.encoding)
        if (ec != 0) {
            log.error("encode request error, serviceId=" + req.serviceId + ", msgId=" + req.msgId)

            val res = createErrorResponse(ec, req)
            router.reply(new RequestResponseInfo(req, res))
            return
        }

        val data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            tlvCodec.version,
            req.serviceId,
            req.msgId,
            sequence,
            0,
            req.encoding,
            0,
            xhead, body)

        val connId = selectConnId(req.toAddr, req.xhead.s(Xhead.KEY_SOC_ID))
        if (connId == null || connId == "") {
            val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION, req)
            router.reply(new RequestResponseInfo(req, res))
            return
        }

        val bb = encode(data, connId)

        val t = qte.newTimer(timeout, sequence)
        dataMap.put(sequence, new CacheData(req, t))

        val ok = nettyServer.write(connId, bb)

        if (!ok) {
            dataMap.remove(sequence)
            t.cancel()
            val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION, req)
            router.reply(new RequestResponseInfo(req, res))
        }

    }

    def send(req: RawRequest, timeout: Int): Unit = {

        val sequence = generateSequence()

        val xhead = TlvCodec4Xhead.decode(req.data.serviceId, req.data.xhead)

        val connId = selectConnId(null, xhead.s(Xhead.KEY_SOC_ID))
        if (connId == null || connId == "") {
            val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION, req)
            router.reply(new RawRequestResponseInfo(req, res))
            return
        }

        val data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            router.codecs.version(req.data.serviceId), // req.data.version,
            req.data.serviceId,
            req.data.msgId,
            sequence,
            0,
            req.data.encoding,
            0,
            EMPTY_BUFFER, req.data.body)

        val bb = encode(data, connId)

        val t = qte.newTimer(timeout, sequence)
        dataMap.put(sequence, new CacheData(req, t))

        val ok = nettyServer.write(connId, bb)

        if (!ok) {
            dataMap.remove(sequence)
            t.cancel()
            val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION, req)
            router.reply(new RawRequestResponseInfo(req, res))
        }

    }

    def onTimeout(data: Any): Unit = {
        val sequence = data.asInstanceOf[Int]
        receive(SosSendTimeout(sequence))
    }

    def receive(bb: ChannelBuffer, connId: String) {

        val data = decode(bb, connId)

        data.flag match {

            case AvenueCodec.TYPE_REQUEST =>

                if (isPing(data.serviceId, data.msgId)) {

                    sendPong(data, connId)

                    return
                }

                // append remote addr to xhead, the last addr is always remote addr
                TlvCodec4Xhead.appendAddr(data.xhead, parseRemoteAddr(connId), isSps, data.version)

                val requestId = RequestIdGenerator.nextId()
                val rr = new RawRequest(requestId, data, connId, this)
                receive(rr)

            case AvenueCodec.TYPE_RESPONSE =>

                if (isAck(data.code)) {

                    try {
                        receive(SosSendAck(data, connId))
                    } catch {
                        case e: Throwable =>
                            log.error("receive exception res={}", data, e)
                    }
                    return
                }

                // append remote addr to xhead, the last addr is always remote addr
                TlvCodec4Xhead.appendAddr(data.xhead, parseRemoteAddr(connId),false,data.version)

                try {
                    receive(SosSendResponse(data, connId))
                } catch {
                    case e: Throwable =>
                        log.error("receive exception res={}", data, e)
                }

            case _ =>

                log.error("unknown type")

        }

    }

    def reply(data: AvenueData, connId: String): Unit = {

        if (connId == null || connId == "") return

        val bb = encode(data, connId)
        nettyServer.write(connId, bb)
    }

    def replyWithErrorCode(code: Int, req: AvenueData, connId: String) {

        if (connId == null || connId == "") return

        val res = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            req.version,
            req.serviceId,
            req.msgId,
            req.sequence,
            0,
            req.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER)

        val bb = converter.encode(res)
        nettyServer.write(connId, bb)
    }

    def replyAck(req: AvenueData, connId: String) {

        if (connId == null || connId == "") return

        val res = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            req.version,
            req.serviceId,
            req.msgId,
            req.sequence,
            0,
            req.encoding,
            AvenueCodec.ACK_CODE, // ack
            EMPTY_BUFFER,
            EMPTY_BUFFER)

        val bb = converter.encode(res)
        nettyServer.write(connId, bb)
    }

    def sendPong(data: AvenueData, connId: String) {

        if (connId == null || connId == "") return

        val res = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            data.version,
            0,
            0,
            data.sequence,
            0,
            data.encoding,
            0,
            EMPTY_BUFFER,
            EMPTY_BUFFER)

        val bb = converter.encode(res)
        nettyServer.write(connId, bb)
    }

    def isPing(serviceId: Int, msgId: Int) = { serviceId == 0 && msgId == 0 }

    def parseRemoteAddr(connId: String): String = {

        val p = connId.lastIndexOf(":")

        if (p >= 0)
            connId.substring(0, p)
        else
            "0.0.0.0:0"
    }
    def parseRemoteIp(connId: String): String = {

        val p = connId.indexOf(":")

        if (p >= 0)
            connId.substring(0, p)
        else
            "0.0.0.0"
    }

    def in(ss: String, s: String, t: String = ","): Boolean = {
        if (ss == null || ss == "") return false
        if (s == null || s == "") return true
        (t + ss + t).indexOf(t + s + t) >= 0
    }

    def decode(bb: ChannelBuffer, connId: String): AvenueData = {
        var data: AvenueData = null
        if (isEncrypted) {
            val key = aesKeyMap.get(connId)
            try {
                data = converter.decode(bb, key)
            } catch {
                case e: CodecException =>
                    log.error("decode error,service=%d,msgId=%d".format(data.serviceId, data.msgId))
                    throw e
            }

            val serviceIdMsgId = data.serviceId + ":" + data.msgId
            if (key == null && !in(shakeHandsServiceIdMsgId, serviceIdMsgId) && serviceIdMsgId != "0:0") { // heartbeat
                log.error("decode error, not shakehanded,service=%d,msgId=%d".format(data.serviceId, data.msgId))
                throw new RuntimeException("not shakehanded")
            }
        } else {
            data = converter.decode(bb)
        }
        data
    }

    def encode(data: AvenueData, connId: String): ChannelBuffer = {
        var bb: ChannelBuffer = null
        if (isEncrypted) {
            var key = aesKeyMap.get(connId)
            val serviceIdMsgId = data.serviceId + ":" + data.msgId
            if (key == null && !in(shakeHandsServiceIdMsgId, serviceIdMsgId)) {
                log.error("encode error, not shakehanded,service=%d,msgId=%d".format(data.serviceId, data.msgId))
                return null
            }
            if (in(shakeHandsServiceIdMsgId, serviceIdMsgId)) key = null
            bb = converter.encode(data, key)
        } else {
            bb = converter.encode(data)
        }
        bb
    }

    case class SosSendAck(data: AvenueData, connId: String)
    case class SosSendResponse(data: AvenueData, connId: String)
    case class SosSendTimeout(sequence: Int)

    class CacheData(val data: AnyRef, val timer: QuickTimer) {
        val sendTime = System.currentTimeMillis
    }

}

class DummySos() extends Sos(null, 0) {

    override def init() {
        log.info("dummysos started")
    }

    override def close() {
        log.info("dummysos stopped")
    }

    override def closeReadChannel() = {}
    override def start() = {}

    override def stats(): Array[Int] = { Array(0, 0, 0) }

    override def receive(v: Any) = {}

    override def dump() = {}

    override def receive(bb: ChannelBuffer, connId: String) = {}
}

