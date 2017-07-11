package scalabpe.core

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import com.sdo.billing.queue.PersistQueue
import com.sdo.billing.queue.impl.PersistQueueManagerImpl

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

case class MustReachReqCommitInfo(req: Request, code: Int)
case class MustReachRawReqCommitInfo(rawReq: RawRequest, code: Int)

object PersistData {
    val SOURCE_RAWREQ = 1
    val SOURCE_REQ = 2
}

class PersistData(
    val source: Int, // 1 from rawRequest 2 for request
    val serviceId: Int,
    val msgId: Int,
    val encoding: Int,
    val createTime: Long,
    val xhead: ChannelBuffer,
    val body: ChannelBuffer) {}

class MustReachSendingData(
        val queueName: String,
        var persistId: Long = 0,
        var data: PersistData = null,
        var sendCount: Int = 0,
        var nextRunTime: Long = 0L) {

    def reset() {
        persistId = 0
        data = null
        sendCount = 0
        nextRunTime = 0L
    }

}

class MustReachRetryConfig(val retryTimes: Int, val retryInterval: Int)

class MustReachActor(val router: Router, val cfgNode: Node) extends Actor with Logging with Closable
        with SelfCheckLike with Dumpable with BeforeClose with AfterInit {

    import scalabpe.core.PersistData._

    val mustReachMsgMap = new HashMap[String, MustReachRetryConfig]() // serviceId:msgId

    val queueTypeName = "mustreach"

    var persistQueueManager: PersistQueueManagerImpl = _

    var retryThread: Thread = _

    val hasIOException = new AtomicBoolean()

    val lock = new ReentrantLock(false)
    val hasNewData = lock.newCondition()
    val sequence = new AtomicInteger(1)

    val waitingRunnableList = new ConcurrentLinkedQueue[Runnable]()
    val waitingQueueNameList = new ConcurrentLinkedQueue[String]()
    val queuesNoData = new HashMap[String, MustReachSendingData]()
    val queuesHasData = new HashMap[String, MustReachSendingData]()

    val shutdown = new AtomicBoolean()
    val beforeCloseFlag = new AtomicBoolean()

    var defaultRetryTimes = 3 * 24 * 60 // 3 days
    var defaultRetryInterval = 60000 // 1 minute

    var maxCacheSize = 5000
    val cachedData = new ConcurrentHashMap[String, Array[Byte]]()

    init

    def genQueueName(serviceId: Int, msgId: Int): String = {
        serviceId + "_" + msgId
    }

    def getRetryTimes(queueName: String): Int = {
        val c = mustReachMsgMap.getOrElse(queueName, null)
        if (c == null) defaultRetryTimes
        else c.retryTimes
    }
    def getRetryInterval(queueName: String): Int = {
        val c = mustReachMsgMap.getOrElse(queueName, null)
        if (c == null) defaultRetryInterval
        else c.retryInterval
    }

    def init() {

        var s = router.getConfig("mustReach.defaultRetryTimes", "")
        if (s != "") defaultRetryTimes = s.toInt
        s = router.getConfig("mustReach.defaultRetryInterval", "")
        if (s != "") defaultRetryInterval = s.toInt
        s = router.getConfig("mustReach.maxCacheSize", "")
        if (s != "") maxCacheSize = s.toInt

        for ((serviceId, codec) <- router.codecs.codecs_id) {

            val msgIds = codec.msgKeyToTypeMapForReq.keys

            for (msgId <- msgIds) {

                val msgAttributes = codec.msgAttributes.getOrElse(msgId, null)
                var s = msgAttributes.getOrElse("isack", "").toLowerCase
                if (s == "") s = msgAttributes.getOrElse("isAck", "").toLowerCase
                if (s == "1" || s == "true" || s == "yes" || s == "t" || s == "y") {

                    var retryTimes = defaultRetryTimes
                    s = msgAttributes.getOrElse("retryTimes", "")
                    if (s != "")
                        retryTimes = s.toInt

                    var retryInterval = defaultRetryInterval
                    s = msgAttributes.getOrElse("retryInterval", "")
                    if (s != "")
                        retryInterval = s.toInt

                    val config = new MustReachRetryConfig(retryTimes, retryInterval)
                    val queueName = genQueueName(serviceId, msgId)
                    mustReachMsgMap.put(queueName, config)
                }
            }

        }

        val dataDir = Router.dataDir + File.separator + queueTypeName
        new File(dataDir).mkdirs()
        persistQueueManager = new PersistQueueManagerImpl()
        persistQueueManager.setDataDir(dataDir)
        persistQueueManager.setCacheSize(0)
        persistQueueManager.init()

        val queueNames = persistQueueManager.getQueueNames
        for (i <- 0 until queueNames.size) {
            waitingQueueNameList.offer(queueNames.get(i))
        }

        retryThread = new Thread(queueTypeName + "_retrythread") {
            override def run() {
                sendData()
            }
        }

        log.info(getClass.getName + " started")
    }

    def afterInit() {
        retryThread.start()
        log.info(getClass.getName + " retryThread started")
    }

    def beforeClose() {
        beforeCloseFlag.set(true)
        log.info(getClass.getName + " beforeClose called")
    }

    def close() {

        shutdown.set(true)
        retryThread.interrupt()
        retryThread.join()

        if (persistQueueManager != null) {
            persistQueueManager.close()
            persistQueueManager = null
        }

        log.info(getClass.getName + " closed")
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var ioErrorId = 65301007

        if (hasIOException.get()) {
            val msg = "local persistqueue has io error"
            buff += new SelfCheckResult("SCALABPE.IO", ioErrorId, true, msg)
        }

        buff
    }

    def dump() {

        val buff = new StringBuilder

        buff.append("retryThread size=1").append(",")
        buff.append("retrytimer thread size=1").append(",")

        buff.append("waitingRunnableList.size=").append(waitingRunnableList.size).append(",")
        buff.append("waitingQueueNameList.size=").append(waitingQueueNameList.size).append(",")
        buff.append("queuesNoData.size=").append(queuesNoData.size).append(",")
        buff.append("queuesHasData.size=").append(queuesHasData.size).append(",")
        buff.append("cachedData.size=").append(cachedData.size).append(",")

        log.info(buff.toString)

        dumpPersistManager
    }

    def dumpPersistManager() {
        val buff1 = new StringBuilder
        val buff2 = new StringBuilder

        buff1.append("queue size: ")
        buff2.append("queue cacheSize: ")
        val queueNames = persistQueueManager.getQueueNames
        for (i <- 0 until queueNames.size) {
            val queue = persistQueueManager.getQueue(queueNames.get(i))
            buff1.append(queueNames.get(i)).append("=").append(queue.size).append(",")
            buff2.append(queueNames.get(i)).append("=").append(queue.cacheSize).append(",")
        }
        log.info(buff1.toString)
        log.info(buff2.toString)
    }

    def isReached(code: Int): Boolean = {
        code != -10242488 && code != -10242504 && code != -10242404
    }

    override def receive(v: Any): Unit = {

        if (v == null) return

        v match {
            case req: Request => // from flow engine, must use the caller's thread to process
                onReceiveRequest(req)
                return
            case rawReq: RawRequest => // from network, must use the caller's thread to process
                onReceiveRawRequest(rawReq)
                return
            case _ =>
                waitingRunnableList.offer(new Runnable() {
                    def run() {
                        onReceiveCommitInfo(v)
                    }
                })

                wakeUpRetryThread()
        }

    }

    def onReceiveRequest(req: Request) {

        if (req.persistId != 0) return

        val queueName = genQueueName(req.serviceId, req.msgId)
        val found = mustReachMsgMap.contains(queueName)
        if (!found) return

        val bs = reqToBytes(req)
        if (bs == null) return
        val persistId = persist(queueName, bs)
        req.persistId = persistId
    }

    def onReceiveRawRequest(rawReq: RawRequest) {

        if (rawReq.persistId != 0) return

        var req = rawReq.data
        val queueName = genQueueName(req.serviceId, req.msgId)
        val found = mustReachMsgMap.contains(queueName)
        if (!found && rawReq.data.mustReach == 0) {
            return
        }

        val bs = reqToBytes(rawReq)
        if (bs == null) return
        val persistId = persist(queueName, bs)
        rawReq.persistId = persistId
    }

    def persist(queueName: String, bs: Array[Byte]): Long = {

        var idx = 0L

        if (bs == null) return idx

        try {
            val queue = persistQueueManager.getQueue(queueName)
            idx = queue.putAndReturnIdx(bs)

            if (cachedData.size < maxCacheSize) {
                cachedData.put(queueName + ":" + idx, bs)
            }

            waitingQueueNameList.offer(queueName)
            wakeUpRetryThread()
            hasIOException.set(false)
        } catch {
            case e: Exception =>
                log.error("cannot save data to must reach queue queueName={}", queueName)
                hasIOException.set(true)
                idx = 0
        }
        if (idx < 0) idx = 0

        idx
    }

    def wakeUpRetryThread() {

        if (lock.tryLock()) {

            try {
                hasNewData.signal()
            } finally {
                lock.unlock()
            }

        }
    }

    def sendData() {

        lock.lock()
        while (!shutdown.get()) {

            try {
                sendDataInternal()

                if (!needRun()) {
                    hasNewData.await(1000, TimeUnit.MILLISECONDS) // ignore return code
                }

            } catch {
                case e: InterruptedException =>

                case e: Throwable =>
                    log.error("exception in sendData, e={}", e.getMessage)
            }

        }

        lock.unlock()
    }

    def addQueueName(queueName: String) {

        var existed = queuesHasData.contains(queueName)
        if (existed) {
            return
        }

        val sendingdata = queuesNoData.getOrElse(queueName, null)
        if (sendingdata != null) {
            queuesNoData.remove(queueName)
            sendingdata.reset
            queuesHasData.put(queueName, sendingdata)
            return
        }

        queuesHasData.put(queueName, new MustReachSendingData(queueName))
    }

    def loadData(queueName: String, sendingdata: MustReachSendingData, queue: PersistQueue): Boolean = {

        var hasData = true

        try {
            val idx = queue.get(0) // no wait

            if (idx == -1) { // no data

                hasData = false

            } else {

                var bs = cachedData.remove(queueName + ":" + idx)
                if (bs == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("queue.getBytes called")
                    }
                    bs = queue.getBytes(idx)
                }
                sendingdata.data = bytesToPersistData(bs)
                if (sendingdata.data != null) {
                    sendingdata.persistId = idx
                    sendingdata.nextRunTime = sendingdata.data.createTime + getRetryInterval(queueName)
                }
            }
        } catch {
            case e: Exception =>
                log.error("exception in sending must reach data {}", e.getMessage)
        }

        hasData
    }

    def needRun() = {
        val now = System.currentTimeMillis

        !waitingRunnableList.isEmpty() ||
            !waitingQueueNameList.isEmpty() ||
            queuesHasData.values.filter(d => d.data == null || d.data != null && d.nextRunTime < now).size > 0
    }

    def sendDataInternal() {

        while (!waitingRunnableList.isEmpty()) {

            val runnable = waitingRunnableList.poll()

            try {
                runnable.run()
            } catch {
                case e: Throwable =>
            }

        }

        while (!waitingQueueNameList.isEmpty()) {
            val queueName = waitingQueueNameList.poll()
            addQueueName(queueName)
        }

        val removeList = new ArrayBuffer[String]()

        for ((queueName, sendingdata) <- queuesHasData if sendingdata.data == null) { // not sending data

            val queue = persistQueueManager.getQueue(queueName)
            if (queue == null) {
                removeList += queueName
            } else {
                val hasData = loadData(queueName, sendingdata, queue)
                if (!hasData)
                    removeList += queueName
            }

        }

        for (queueName <- removeList) {
            val sendingdata = queuesHasData.getOrElse(queueName, null)
            queuesHasData.remove(queueName)
            sendingdata.reset
            queuesNoData.put(queueName, sendingdata)
        }

        val now = System.currentTimeMillis
        for ((queueName, sendingdata) <- queuesHasData if sendingdata.data != null && sendingdata.nextRunTime < now) {

            val ok = send(sendingdata)
            if (!ok) {
                val queue = persistQueueManager.getQueue(queueName)
                queue.commit(sendingdata.persistId)
                cachedData.remove(queueName + ":" + sendingdata.persistId)
                sendingdata.reset
            }
        }
    }

    def send(sendingdata: MustReachSendingData): Boolean = {

        if (beforeCloseFlag.get()) {
            return true
        }

        val requestId = "MR" + RequestIdGenerator.nextId()
        val nextSeq = sequence.getAndIncrement()

        val data = sendingdata.data

        data.source match {

            case SOURCE_RAWREQ =>

                val newdata = new AvenueData(
                    AvenueCodec.TYPE_REQUEST,
                    router.codecs.version(data.serviceId),
                    data.serviceId,
                    data.msgId,
                    nextSeq,
                    AvenueCodec.MUSTREACH_YES,
                    data.encoding,
                    0,
                    data.xhead,
                    data.body)

                val rawreq = new RawRequest(requestId, newdata, Router.DO_NOT_REPLY, this)
                rawreq.persistId = sendingdata.persistId
                router.send(rawreq)
                sendingdata.nextRunTime = System.currentTimeMillis + getRetryInterval(sendingdata.queueName)

            case SOURCE_REQ =>

                //try {

                val xhead = TlvCodec4Xhead.decode(data.serviceId, data.xhead)

                val tlvCodec = router.findTlvCodec(data.serviceId)
                if (tlvCodec == null) {
                    return false
                }

                val (body, ec) = tlvCodec.decodeRequest(data.msgId, data.body, data.encoding)
                if (ec != 0) return false

                val req = new Request(
                    requestId,
                    Router.DO_NOT_REPLY,
                    nextSeq,
                    data.encoding,
                    data.serviceId,
                    data.msgId,
                    xhead,
                    body,
                    this)

                req.persistId = sendingdata.persistId
                router.send(req)
                sendingdata.nextRunTime = System.currentTimeMillis + getRetryInterval(sendingdata.queueName)

            //} catch {
            //case e:Exception =>
            //return false
            //}

            case _ =>

        }

        true
    }

    def onReceiveCommitInfo(v: Any) {

        try {

            v match {
                case MustReachReqCommitInfo(req, code) =>
                    if (req.persistId == 0) return
                    commit(req.serviceId, req.msgId, req.persistId, code)

                case MustReachRawReqCommitInfo(rawReq, code) =>
                    if (rawReq.persistId == 0) return
                    commit(rawReq.data.serviceId, rawReq.data.msgId, rawReq.persistId, code)

                case _ =>
            }

        } catch {
            case e: Exception =>
                log.error(getClass.getName + " exception req={}", v.toString, e)
        }

    }

    def commit(serviceId: Int, msgId: Int, persistId: Long, code: Int) {

        val queueName = genQueueName(serviceId, msgId)
        val sendingdata = queuesHasData.getOrElse(queueName, null)
        val retryTimes = getRetryTimes(queueName)

        var reached = isReached(code)
        var isSending = false
        var retryOver = false

        if (sendingdata != null && sendingdata.data != null && sendingdata.persistId == persistId) {
            isSending = true
            if (sendingdata.sendCount >= retryTimes) retryOver = true
        }

        cachedData.remove(queueName + ":" + persistId)

        if (reached || retryOver) {

            commit(queueName, persistId)

            if (isSending) {
                sendingdata.reset()
            }

        }

    }

    def commit(queueName: String, idx: Long) {

        val queue = persistQueueManager.getQueue(queueName)
        if (queue == null) {
            return
        } else {
            try {
                queue.commit(idx)
            } catch {
                case e: Exception =>
                    log.error("exception in commit must reach data {}", e.getMessage)
            }
        }
    }

    def reqToBytes(v: Any): Array[Byte] = {

        var pd: PersistData = null

        v match {
            case req: Request =>
                pd = toPersistData(req)
            case rawReq: RawRequest =>
                pd = toPersistData(rawReq)
            case _ =>
        }
        if (pd == null) return null

        persistDataToBytes(pd)
    }

    def toPersistData(rawReq: RawRequest): PersistData = {

        new PersistData(SOURCE_RAWREQ,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            rawReq.data.encoding,
            System.currentTimeMillis,
            rawReq.data.xhead,
            rawReq.data.body)
    }

    def toPersistData(req: Request): PersistData = {

        val tlvCodec = router.codecs.findTlvCodec(req.serviceId)
        if (tlvCodec == null) {
            return null
        }

        val version = router.codecs.version(req.serviceId)

        val xhead = TlvCodec4Xhead.encode(req.serviceId, req.xhead, version)

        val (body, ec) = tlvCodec.encodeRequest(req.msgId, req.body, req.encoding)
        if (ec != 0) return null

        val pd = new PersistData(SOURCE_REQ,
            req.serviceId,
            req.msgId,
            req.encoding,
            System.currentTimeMillis,
            xhead,
            body)

        pd
    }

    def persistDataToBytes(pd: PersistData): Array[Byte] = {

        val xheadcopy = pd.xhead.duplicate()
        val bodycopy = pd.body.duplicate()

        val len = 18 + 4 + xheadcopy.writerIndex + 4 + bodycopy.writerIndex
        val buff = ChannelBuffers.buffer(len)

        buff.writeByte(pd.source.toByte)
        buff.writeInt(pd.serviceId)
        buff.writeInt(pd.msgId)
        buff.writeByte(pd.encoding.toByte)
        buff.writeLong(pd.createTime)

        buff.writeInt(xheadcopy.writerIndex)
        buff.writeBytes(xheadcopy)
        buff.writeInt(bodycopy.writerIndex)
        buff.writeBytes(bodycopy)

        val a = new Array[Byte](buff.writerIndex)
        buff.readBytes(a)
        a
    }

    def bytesToPersistData(bs: Array[Byte]): PersistData = {

        try {
            val buff = ChannelBuffers.wrappedBuffer(bs)

            val source = buff.readByte()
            val serviceId = buff.readInt()
            val msgId = buff.readInt()
            val encoding = buff.readByte()
            val createTime = buff.readLong()

            val xheadlen = buff.readInt()
            val xhead = buff.readBytes(xheadlen)
            
            val bodylen = buff.readInt()
            val body = buff.readBytes(bodylen)

            val pd = new PersistData(source,
                serviceId,
                msgId,
                encoding,
                createTime,
                xhead,
                body)

            pd

        } catch {
            case e: Throwable =>
                return null
        }
    }

}
