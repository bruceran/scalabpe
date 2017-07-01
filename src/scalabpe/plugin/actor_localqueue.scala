package scalabpe.plugin

import java.io.File
import java.io.StringWriter
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.xml.Node

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.sdo.billing.queue.PersistQueue
import com.sdo.billing.queue.impl.PersistQueueManagerImpl

import scalabpe.core.Actor
import scalabpe.core.AfterInit
import scalabpe.core.BeforeClose
import scalabpe.core.Closable
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.InvokeResult
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.Request
import scalabpe.core.RequestIdGenerator
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.Router
import scalabpe.core.SelfCheckLike
import scalabpe.core.SelfCheckResult

object LocalQueueActor {
    val localDirs = new HashSet[String]()
}

class MsgPaCfg(val maxSendTimes: Int, val retryInterval: Int, val concurrentNum: Int = 0)

class LocalQueueActor(override val router: Router, override val cfgNode: Node)
        extends LocalQueueLike(router, cfgNode) {

    val queueNameMap = new HashMap[String, String]() // serviceId:msgId -> queueNameKey

    init

    override def init() {
        queueTypeName = "localqueue"
        serviceIds = (cfgNode \ "ServiceId").text

        super.init

        var infos = cfgNode \ "Msg"
        for (inf <- infos) {
            var msgId = (inf \ "@msgId").text
            var s = (inf \ "@retryInterval").toString()
            var retryIntervalCfg = if (s != "") s.toInt else 0
            s = (inf \ "@maxSendTimes").toString()
            var maxSendTimesCfg = if (s != "") s.toInt else 0
            s = (inf \ "@concurrentNum").toString()
            var concurrentNum = if (s != "") s.toInt else 0

            msgIdCfgMap.put(msgId, new MsgPaCfg(maxSendTimesCfg, retryIntervalCfg, concurrentNum))

        }

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for (serviceId <- serviceIdArray) {
            val codec = router.codecs.findTlvCodec(serviceId)

            if (codec == null) {
                throw new RuntimeException("serviceId not found, serviceId=" + serviceId)
            }

            if (codec != null) {
                val tlvType = codec.findTlvType(10000)
                if (tlvType == null) {
                    throw new RuntimeException("queueName not configured for serviceId=" + serviceId)
                }
                val msgIds = codec.msgKeyToTypeMapForReq.keys

                for (msgId <- msgIds) {

                    val reqNameMap = codec.msgKeysForReq.getOrElse(msgId, null)
                    val keyToTypeMapReq = codec.msgKeyToTypeMapForReq.getOrElse(msgId, null)
                    var found = false
                    for (key <- reqNameMap) {

                        val typeKey = keyToTypeMapReq.getOrElse(key, null)
                        val tlvType = codec.typeNameToCodeMap.getOrElse(typeKey, null)
                        if (tlvType.code == 10000) {
                            found = true
                            queueNameMap.put(serviceId + ":" + msgId, key)

                        }
                    }
                    if (!found) {
                        throw new RuntimeException("queueName not configured for serviceId=%d,msgId=%d".format(serviceId, msgId))
                    }
                }

            }

        }

    }

    override def checkLocalDir() {

        localDir = (cfgNode \ "LocalDir").text
        if (localDir == "") {
            localDir = Router.dataDir + File.separator + queueTypeName
        }

        if (LocalQueueActor.localDirs.contains(localDir)) {
            throw new RuntimeException("LocalQueueActor localDir cannot be the same, the default is data/" + queueTypeName)
        }

        LocalQueueActor.localDirs.add(localDir)
    }

    override def onReceive(v: Any) {

        v match {

            case req: Request =>
                onReceiveRequest(req)
            case _ =>
                super.onReceive(v)

        }
    }

    def onReceiveRequest(req: Request) {

        val queueNameKey = queueNameMap.getOrElse(req.serviceId + ":" + req.msgId, null)
        if (queueNameKey == null) {
            log.error("queueName not found serviceId=%d,msgId=%d".format(req.serviceId, req.msgId))
            replyError(ResultCodes.SERVICE_INTERNALERROR, req)
            return
        }

        val queueName = req.s(queueNameKey)
        if (queueName == null) {
            log.error("queueName not found serviceId=%d,msgId=%d".format(req.serviceId, req.msgId))
            replyError(ResultCodes.SERVICE_INTERNALERROR, req)
            return
        }

        val s = requestToJson(req)

        val ok = saveToQueue(queueName, s)
        if (ok)
            replyOk(req)
        else
            replyError(ResultCodes.SERVICE_INTERNALERROR, req)
    }

}

class LocalQueueSendingData(val queueName: String, var requestId: String = null, var idx: Long = 0,
                            var json: String = null, var sendCount: Int = 1) {

    var createTime = System.currentTimeMillis

    def reset() {
        createTime = System.currentTimeMillis
        requestId = null
        idx = 0
        json = null
        sendCount = 1
    }
}

abstract class LocalQueueLike(val router: Router, val cfgNode: Node) extends Actor with Logging
        with Closable with BeforeClose with AfterInit with SelfCheckLike with Dumpable {

    var queueTypeName: String = _
    var serviceIds: String = _
    var localDir: String = _

    var threadNum = 1
    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    var persistQueueManager: PersistQueueManagerImpl = _

    var timer: Timer = _
    var sendThread: Thread = _
    var receiverServiceId = 0

    var maxSendTimes = 60
    var retryInterval = 5000

    val jsonFactory = new JsonFactory()
    val mapper = new ObjectMapper()

    val hasIOException = new AtomicBoolean()

    val lock = new ReentrantLock(false)
    val hasNewData = lock.newCondition()
    val sequence = new AtomicInteger(1)

    val waitingRunnableList = new ConcurrentLinkedQueue[Runnable]()
    val waitingQueueNameList = new ConcurrentLinkedQueue[String]()
    val queuesNoData = new HashMap[String, LocalQueueSendingData]()
    val queuesHasData = new HashMap[String, LocalQueueSendingData]()
    val requestIdMap = new ConcurrentHashMap[String, LocalQueueSendingData]() // requestId -> LocalQueueSendingData

    val shutdown = new AtomicBoolean()
    val beforeCloseFlag = new AtomicBoolean()

    val msgIdCfgMap = new HashMap[String, MsgPaCfg]() //msgid -> maxSendTimes,retryInterval

    def dump() {

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")
        buff.append("sendThread size=1").append(",")
        buff.append("retrytimer thread size=1").append(",")

        buff.append("waitingRunnableList.size=").append(waitingRunnableList.size).append(",")
        buff.append("waitingQueueNameList.size=").append(waitingQueueNameList.size).append(",")
        buff.append("queuesNoData.size=").append(queuesNoData.size).append(",")
        buff.append("queuesHasData.size=").append(queuesHasData.size).append(",")
        buff.append("requestIdMap.size=").append(requestIdMap.size).append(",")
        buff.append("queueNameCfgMap.size=").append(msgIdCfgMap.size).append(",")

        log.info(buff.toString)

        dumpPersistManager
    }

    def dumpPersistManager() {
        val buff1 = new StringBuilder
        val buff2 = new StringBuilder

        buff1.append("queue size ")
        buff2.append("queue cacheSize ")
        val queueNames = persistQueueManager.getQueueNames
        for (i <- 0 until queueNames.size) {
            val queue = persistQueueManager.getQueue(queueNames.get(i))
            buff1.append(queueNames.get(i)).append("=").append(queue.size).append(",")
            buff2.append(queueNames.get(i)).append("=").append(queue.cacheSize).append(",")
        }
        log.info(buff1.toString)
        log.info(buff2.toString)
    }

    def init() {

        checkLocalDir()

        timer = new Timer(queueTypeName + "_retrytimer")

        var dataDir = ""
        if (localDir.startsWith("/")) dataDir = localDir
        else dataDir = router.rootDir + File.separator + localDir

        var s = (cfgNode \ "@maxSendTimes").text
        if (s != "") maxSendTimes = s.toInt
        s = (cfgNode \ "@retryInterval").text
        if (s != "") retryInterval = s.toInt
        s = (cfgNode \ "@threadNum").text
        if (s != "") threadNum = s.toInt

        new File(dataDir).mkdirs()
        persistQueueManager = new PersistQueueManagerImpl()
        persistQueueManager.setDataDir(dataDir)
        persistQueueManager.init()

        val queueNames = persistQueueManager.getQueueNames
        for (i <- 0 until queueNames.size) {
            waitingQueueNameList.offer(queueNames.get(i))
        }

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory(queueTypeName + "_" + firstServiceId)
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        receiverServiceId = (cfgNode \ "@receiverServiceId").text.toInt
        if (receiverServiceId <= 0)
            throw new RuntimeException("receiverServiceId is not valid receiverServiceId=" + receiverServiceId)

        sendThread = new Thread(queueTypeName + "_sedingthread" + firstServiceId) {
            override def run() {
                sendData()
            }
        }

        log.info(getClass.getName + " started {}", serviceIds)
    }

    def afterInit() {

        sendThread.start()
        log.info(getClass.getName + " sendThread started")
    }

    def beforeClose() {
        beforeCloseFlag.set(true)
        log.info(getClass.getName + " beforeClose called")
    }

    def checkLocalDir()

    def close() {

        shutdown.set(true)

        timer.cancel()

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5, TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn(getClass.getName + " long time to shutdown pool, ts={}", t2 - t1)

        sendThread.interrupt()
        sendThread.join()

        if (persistQueueManager != null) {
            persistQueueManager.close()
            persistQueueManager = null
        }

        log.info(getClass.getName + " closed {} ", serviceIds)
    }

    override def receive(v: Any): Unit = {

        try {

            pool.execute(new Runnable() {
                def run() {

                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error(getClass.getName + " exception req={}", v.toString, e)
                    }

                }
            })

        } catch {
            case e: RejectedExecutionException =>
                if (v.isInstanceOf[Request])
                    replyError(ResultCodes.SERVICE_FULL, v.asInstanceOf[Request])
                log.error(getClass.getName + " queue is full, serviceIds={}", serviceIds)
        }

    }

    def replyOk(req: Request) {
        val res = new Response(0, new HashMapStringAny(), req)
        router.reply(new RequestResponseInfo(req, res))
    }

    def replyError(code: Int, req: Request) {
        val res = new Response(code, new HashMapStringAny(), req)
        router.reply(new RequestResponseInfo(req, res))
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

    def onReceive(v: Any) {

        v match {

            case res: InvokeResult =>
                onReceiveResponse(res)
            case _ =>
                log.error("unknown msg")

        }
    }

    def getMaxSendTimes(msgId: Int): Int = {
        val tempvalue = msgIdCfgMap.getOrElse(msgId.toString, null)
        if (tempvalue != null) {
            val c = if (tempvalue.maxSendTimes <= 0) maxSendTimes else tempvalue.maxSendTimes
            return c
        } else {
            return maxSendTimes
        }
    }

    def getRetryInterval(msgId: Int): Int = {
        val tempvalue = msgIdCfgMap.getOrElse(msgId.toString, null)
        if (tempvalue != null) {
            val c = if (tempvalue.retryInterval <= 0) retryInterval else tempvalue.retryInterval
            return c
        } else {
            return retryInterval
        }
    }

    def onReceiveResponse(res: InvokeResult) {
        val sendingdata = requestIdMap.remove(res.requestId)
        val body = jsonToBody(sendingdata.json)
        if (body == null) return

        val msgId = body.i("X-MSGID")
        if (msgId <= 0) {
            log.error("X-MSGID not found or not valid in json " + sendingdata.json)
            return
        }
        val maxSendTimes = getMaxSendTimes(msgId)
        val retryInterval = getRetryInterval(msgId)

        if (sendingdata == null) return
        if (res.code == 0 || sendingdata.sendCount >= maxSendTimes) {

            if (res.code != 0) {
                log.error("send failed, requestId=" + sendingdata.requestId)
            }

            waitingRunnableList.offer(
                new Runnable() {
                    def run() {
                        commit(sendingdata.queueName, sendingdata.idx)
                        sendingdata.reset()
                    }
                })

            wakeUpSendThread()
            return

        }

        timer.schedule(new TimerTask() {
            def run() {

                waitingRunnableList.offer(
                    new Runnable() {
                        def run() {
                            retry(sendingdata)
                        }
                    })
                wakeUpSendThread()

            }
        }, retryInterval)

    }

    def wakeUpSendThread() {

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
                    log.error("exception in sendData, e={}", e.getMessage, e)
            }

        }

        lock.unlock()
    }

    def needRun() = {
        !waitingRunnableList.isEmpty() ||
            !waitingQueueNameList.isEmpty() ||
            queuesHasData.values.filter(_.requestId == null).size > 0
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

        queuesHasData.put(queueName, new LocalQueueSendingData(queueName))
    }

    def checkAndSend(queueName: String, sendingdata: LocalQueueSendingData, queue: PersistQueue): Boolean = {

        var hasData = true

        try {
            val idx = queue.get(0) // no wait

            if (idx == -1) { // no data

                hasData = false

            } else {

                val json = queue.getString(idx)

                sendingdata.json = json
                sendingdata.idx = idx

                val ok = send(sendingdata, null)
                if (!ok) {
                    queue.commit(idx)
                    sendingdata.reset
                }

            }
        } catch {
            case e: Exception =>
                log.error("exception in sending localqueue data {}", e)
        }

        hasData
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

        for ((queueName, sendingdata) <- queuesHasData if sendingdata.requestId == null) { // not sending data

            val queue = persistQueueManager.getQueue(queueName)
            if (queue == null) {
                removeList += queueName
            } else {
                val hasData = checkAndSend(queueName, sendingdata, queue)
                if (!hasData)
                    removeList += queueName
            }

        }

        removeNoDataQueues(removeList)
    }

    def removeNoDataQueues(removeList: ArrayBuffer[String]) {
        for (queueName <- removeList) {
            val sendingdata = queuesHasData.getOrElse(queueName, null)
            queuesHasData.remove(queueName)
            sendingdata.reset
            queuesNoData.put(queueName, sendingdata)
        }
    }

    def retry(sendingdata: LocalQueueSendingData) {
        sendingdata.sendCount += 1
        send(sendingdata)
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
                    log.error("exception in commit localqueue data {}", e.getMessage)
            }
        }
    }

    def send(sendingdata: LocalQueueSendingData, generatedRequestId: String = null): Boolean = {

        //println("send called,idx="+sendingdata.idx)
        if (beforeCloseFlag.get()) {
            return true
        }

        val body = jsonToBody(sendingdata.json)
        if (body == null) return false

        val msgId = body.i("X-MSGID")
        if (msgId <= 0) {
            log.error("X-MSGID not found or not valid in json " + sendingdata.json)
            return false
        }
        body.remove("X-MSGID")

        val maxSendTimes = getMaxSendTimes(msgId)

        body.put("x_sendCount", sendingdata.sendCount)
        body.put("x_isLastSend", if (sendingdata.sendCount == maxSendTimes) 1 else 0)
        body.put("x_maxSendTimes", maxSendTimes)
        body.put("x_sendTimeUsed", System.currentTimeMillis - sendingdata.createTime)

        var requestId = generatedRequestId
        if (requestId == null) {
            requestId = "LQ" + RequestIdGenerator.nextId()
        }
        sendingdata.requestId = requestId
        requestIdMap.put(requestId, sendingdata)

        val req = new Request(
            requestId,
            "localqueue:0",
            sequence.getAndIncrement(),
            1,
            receiverServiceId,
            msgId,
            new HashMapStringAny(),
            body,
            this)

        router.send(req)

        true
    }

    def jsonToBody(json: String): HashMapStringAny = {

        val body = new HashMapStringAny()

        try {

            if (!json.startsWith("{")) {
                log.error("not a valid json, json=" + json)
                return null
            }

            val valueTree = mapper.readTree(json)
            val names = valueTree.fieldNames

            while (names.hasNext) {
                val name = names.next()
                body.put(name, valueTree.get(name).asText)
            }

        } catch {
            case e: Throwable =>
                log.error("not a valid json " + json)
                return null
        }

        body
    }

    def requestToJson(req: Request): String = {
        bodyToJson(req.body, req.msgId)
    }

    def bodyToJson(body: HashMapStringAny, msgId: Int): String = {

        val writer = new StringWriter()

        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        jsonGenerator.writeNumberField("X-MSGID", msgId)

        for ((key, value) <- body if key != "queueName") {

            value match {
                case s: String =>
                    jsonGenerator.writeStringField(key, s)
                case i: Int =>
                    jsonGenerator.writeNumberField(key, i)
                case _ =>
                    jsonGenerator.writeStringField(key, value.toString)
            }

        }

        jsonGenerator.writeEndObject()
        jsonGenerator.close()

        writer.toString()
    }

    def saveToQueue(queueName: String, s: String): Boolean = {

        try {
            val queue = persistQueueManager.getQueue(queueName)
            queue.put(s)
            waitingQueueNameList.offer(queueName)
            wakeUpSendThread()
            hasIOException.set(false)
            return true
        } catch {
            case e: Exception =>
                log.error("cannot save data to local queue data={}", s)
                hasIOException.set(true)
                return false
        }

    }

}


