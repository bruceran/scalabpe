package scalabpe.core
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

case class FlowCallTimeout(requestId: String)
case class FlowTimeout(requestId: String)

class InvokeInfo(val service: String, val timeout: Int, val params: HashMapStringAny, val toAddr: String = null)

object FlowActor {
    val count = new AtomicInteger(1)
}
class FlowActor(val router: Router, var threadNum: Int = 0) extends Actor with Logging with Closable with Dumpable {

    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    val flowmaps = new ConcurrentHashMap[String, Flow]()

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")
        buff.append("flowmaps.size=").append(flowmaps.size).append(",")

        log.info(buff.toString)
    }

    def init() {

        if (threadNum == 0)
            threadNum = (router.cfgXml \ "ThreadNum").text.toInt

        threadFactory = new NamedThreadFactory("flow" + FlowActor.count.getAndIncrement())
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        Flow.router = router

        log.info("FlowActor started")
    }

    def close() {

        val size = flowmaps.size

        val t11 = System.currentTimeMillis
        while (flowmaps.size > 0) {
            Thread.sleep(15)
        }
        val t22 = System.currentTimeMillis
        if (t22 - t11 > 100)
            log.warn("FlowActor long time to wait for flow finished, size={}, ts={}", size, t22 - t11)

        val t1 = System.currentTimeMillis

        pool.shutdown()
        pool.awaitTermination(5, TimeUnit.SECONDS)
        pool = null
        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn("FlowActor long time to shutdown pool, ts={}", t2 - t1)

        log.info("FlowActor stopped")
    }

    override def receive(v: Any) {

        if (Flow.isSyncedInvoke.get() == true) {
            try {
                onReceive(v)
            } catch {
                case e: Exception =>
                    log.error("flow exception v={}", v, e)
            }

        } else {

            if (pool == null) return

            try {
                pool.execute(new Runnable() {
                    def run() {
                        try {
                            onReceive(v)
                        } catch {
                            case e: Exception =>
                                log.error("flow exception v={}", v, e)
                        }
                    }
                })
            } catch {
                case e: RejectedExecutionException =>
                    // ignore the message
                    log.error("flow queue is full")
            }
        }
    }

    def onReceive(v: Any) {

        v match {

            case req: Request =>

                //log.info("request received, req={}",req)

                val flow = createFlow(req)
                if (flow != null) {

                    try {
                        saveFlow(req.requestId, flow)

                        flow.requestReceived()

                    } catch {
                        case e: Exception =>
                            if (!flow.replied) {
                                replyError(ResultCodes.SERVICE_INTERNALERROR, req)
                            }
                            log.error("exception in flow.requestReceived, e={}", e.getMessage, e)
                            removeFlow(req.requestId)
                            flow.cancelFlowTimer()
                    }

                } else {
                    replyError(ResultCodes.SERVICE_NOT_FOUND, req)
                }

            case res: InvokeResult =>

                //log.info("result received, res={}",res)

                val flow = findFlow(Flow.parseRequestId(res.requestId))
                if (flow != null) {

                    try {
                        flow.responseReceived(res)
                    } catch {
                        case e: Exception =>
                            log.error("exception in flow.responseReceived, e={}", e.getMessage, e)
                            if (!flow.replied) {
                                replyError(ResultCodes.SERVICE_INTERNALERROR, flow.req)
                            }
                            removeFlow(flow.req.requestId)
                            flow.cancelFlowTimer()
                    }

                }

            case FlowCallTimeout(requestId) =>

                //log.info("asynccall timeout, requestId={}",requestId)

                val flow = findFlow(Flow.parseRequestId(requestId))
                if (flow != null) {
                    try {
                        flow.timeoutReceived(requestId)
                    } catch {
                        case e: Exception =>
                            log.error("exception in flow.timeoutReceived, e={}", e.getMessage, e)
                            if (!flow.replied) {
                                replyError(ResultCodes.SERVICE_INTERNALERROR, flow.req)
                            }
                            removeFlow(flow.req.requestId)
                            flow.cancelFlowTimer()
                    }
                }

            case FlowTimeout(requestId) =>

                //log.info("flow timeout, requestId={}",requestId)
                removeFlow(requestId)

            case _ =>

                log.error("unknown msg received")

        }
    }

    def findFlow(requestId: String): Flow = {
        val existedFlow = flowmaps.get(requestId)
        existedFlow
    }

    def createFlow(req: Request): Flow = {

        val (serviceName, msgName) = router.serviceIdToName(req.serviceId, req.msgId)
        val clsName = "scalabpe.flow.Flow_" + serviceName + "_" + msgName
        try {
            val flow = Class.forName(clsName).newInstance.asInstanceOf[Flow]

            Flow.updateClsStats(clsName)

            flow.req = req
            flow.owner = this
            flow
        } catch {
            case e: Exception =>
                log.error("class not found, clsName={},e={}", clsName, e.getMessage);
                null
        }
    }

    def saveFlow(requestId: String, flow: Flow) {
        flowmaps.put(requestId, flow)
    }

    def removeFlow(requestId: String): Flow = {
        flowmaps.remove(requestId)
    }

    def replyError(errorCode: Int, req: Request) {

        val res = new Response(errorCode, new HashMapStringAny(), req)

        router.reply(new RequestResponseInfo(req, res))
    }

}

object Flow {

    var router: Router = _

    val callStats = new ConcurrentHashMap[String, String]()

    val isSyncedInvoke = new ThreadLocal[Boolean]() {
        override def initialValue(): Boolean = {
            return false
        }
    }

    def parseRequestId(requestId: String): String = {
        val p = requestId.lastIndexOf(":");
        if (p < 0) requestId
        else requestId.substring(0, p)
    }

    def parseIdx(requestId: String): Int = {
        val p = requestId.lastIndexOf(":");
        if (p < 0)
            0
        else
            requestId.substring(p + 1).toInt
    }

    def newRequestId(requestId: String, idx: Int): String = {
        requestId + ":" + idx
    }

    def updateClsStats(clsName: String) {
        if (!Router.testMode) return

        val key = clsName + ":0"
        val value = clsName + ":constructor:unknown:0"

        callStats.putIfAbsent(key, value)
    }

    def updateCallStats() {
        if (!Router.testMode) return

        val elements = Thread.currentThread().getStackTrace()
        for (e <- elements) {
            val clsName = e.getClassName()
            val methodName = e.getMethodName()
            val fileName = e.getFileName()
            val lineNumber = e.getLineNumber()
            if (clsName.indexOf("scalabpe.flow.") >= 0) {
                val key = clsName + ":" + lineNumber
                val value = clsName + ":" + methodName + ":" + fileName + ":" + lineNumber
                callStats.putIfAbsent(key, value)
            }
        }
    }
}

abstract class Flow extends Logging {

    var req: Request = _
    protected[core] var owner: FlowActor = _

    protected[core] var replied = false
    var flowTimeout = 60000

    private var flowFinished = true
    private var flowTimer: QuickTimer = _

    private val callTimers = new HashMap[String, QuickTimer]()
    private var logVars: HashMapStringAny = null
    private var lastf: () => Unit = _
    private var idx = 0

    protected[core] var lastresultarray: ArrayBuffer[InvokeResult] = _
    protected[core] var subrequestIds: ArrayBufferString = _
    protected[core] private val idxmap = new HashMap[Int, Int]()
    protected[core] val lock = new ReentrantLock(false)
    protected[core] val allResultsReceived = lock.newCondition();
    protected[core] var replyOnError = false
    protected[core] var replyOnErrorCode = 0

    // 可重载点
    def filterRequest(map: HashMapStringAny): Unit = {}
    def baseReceive(): Unit = { receive() }
    def filterInvoke(targetServiceId: Int, targetMsgId: Int, map: HashMapStringAny): Unit = {}
    def filterReply(code: Int, map: HashMapStringAny): Unit = {}
    def baseEndFlow(): Unit = {}

    def receive(): Unit; // user defined flow

    def getConfig(name: String, defaultValue: String = ""): String = {
        Flow.router.getConfig(name, defaultValue)
    }

    def logVar(name: String, value: Any) {
        if (logVars == null) logVars = new HashMapStringAny()
        logVars.put(name, value)
    }

    private def nextRequestId(): String = {
        val subrequestid = Flow.newRequestId(req.requestId, idx)
        idxmap.put(idx, 0)
        idx += 1
        subrequestid
    }

    private def nextRequestIds(n: Int): ArrayBufferString = {

        val ids = new ArrayBufferString();
        for (i <- 0 until n) {
            val subrequestid = Flow.newRequestId(req.requestId, idx)
            idxmap.put(idx, i)
            idx += 1
            ids += subrequestid
        }

        ids
    }

    protected[core] def cancelFlowTimer(): Unit = {
        flowTimer.cancel()
    }

    def autoReply(): Unit = {
        if (replied) return
        reply(lasterrorcode())
    }

    private def endFlow(): Unit = {
        autoReply()

        flowFinished = true
        baseEndFlow()

        if (!flowFinished) {
            return
        }

        flowTimer.cancel()
        owner.removeFlow(req.requestId)
    }

    private def doNothingCallback(): Unit = {}

    protected[core] def requestReceived(): Unit = {

        Flow.updateCallStats()

        flowTimer = Flow.router.newTimer(Flow.router.timeoutId(FlowTimoutType.TYPE_FLOW, req.serviceId, req.msgId, req.requestId), flowTimeout)

        flowFinished = true

        lock.lock();

        try {
            filterRequest(req.body)

            baseReceive()

            if (flowFinished) {
                endFlow()
            }

        } finally {
            lock.unlock()
        }

    }

    protected[core] def timeoutReceived(requestId: String): Unit = {
        responseReceived(InvokeResult.timeout(requestId))
    }

    protected[core] def responseReceived(res: InvokeResult): Unit = {

        lock.lock();

        try {
            responseReceivedInternal(res)
        } finally {
            lock.unlock()
        }

    }

    def setInvokeResultInternal(res: InvokeResult, arrayidx: Int = 0) = {
        lastresultarray(arrayidx) = res
    }

    private def responseReceivedInternal(res: InvokeResult): Unit = {

        val idx = Flow.parseIdx(res.requestId)

        val arrayidx = idxmap.getOrElse(idx, -1)
        if (arrayidx == -1) {
            return
        }

        idxmap.remove(idx)

        lastresultarray(arrayidx) = res

        val timer = callTimers.remove(res.requestId)
        if (timer != None) {
            timer.get.cancel()
        }

        val finished = lastresultarray.forall(_ != null)

        if (finished) {

            if (lastf != null) {

                val t = lastf
                lastf = null

                flowFinished = true
                if (!doReplyOnError()) {
                    t()
                }

                if (flowFinished) {
                    endFlow()
                }

            } else {

                lock.lock();

                try {
                    allResultsReceived.signal()
                } finally {
                    lock.unlock();
                }

            }
        }

    }

    def invokeWithNoReply(service: String, params: Tuple2[String, Any]*): Unit = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, 0, body)
        invokeMulti(doNothingCallback, ArrayBuffer[InvokeInfo](info))
    }
    def invokeWithNoReplyWithToAddr(service: String, toAddr: String, params: Tuple2[String, Any]*): Unit = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, 0, body, toAddr)
        invokeMulti(doNothingCallback, ArrayBuffer[InvokeInfo](info))
    }

    def invokeFuture(service: String, timeout: Int, params: Tuple2[String, Any]*): InvokeInfo = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        new InvokeInfo(service, timeout, body)
    }
    def invokeFutureWithToAddr(service: String, timeout: Int, toAddr: String, params: Tuple2[String, Any]*): InvokeInfo = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        new InvokeInfo(service, timeout, body, toAddr)
    }

    def invoke(f: () => Unit, service: String, timeout: Int, params: Tuple2[String, Any]*): Unit = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, timeout, body)
        invokeMulti(f, ArrayBuffer[InvokeInfo](info))
    }
    def invokeAutoReply(f: () => Unit, service: String, timeout: Int, params: Tuple2[String, Any]*): Unit = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, timeout, body)
        invokeMultiAutoReply(f, ArrayBuffer[InvokeInfo](info))
    }
    def invokeWithToAddr(f: () => Unit, service: String, timeout: Int, toAddr: String, params: Tuple2[String, Any]*): Unit = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, timeout, body, toAddr)
        invokeMulti(f, ArrayBuffer[InvokeInfo](info))
    }

    def invoke2(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2)
        invokeMulti(f, infos)
    }
    def invoke2AutoReply(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2)
        invokeMultiAutoReply(f, infos)
    }
    def invoke3(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3)
        invokeMulti(f, infos)
    }
    def invoke3AutoReply(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3)
        invokeMultiAutoReply(f, infos)
    }
    def invoke4(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo, info4: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3, info4)
        invokeMulti(f, infos)
    }
    def invoke4AutoReply(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo, info4: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3, info4)
        invokeMultiAutoReply(f, infos)
    }
    def invoke5(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo, info4: InvokeInfo, info5: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3, info4, info5)
        invokeMulti(f, infos)
    }
    def invoke5AutoReply(f: () => Unit, info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo, info4: InvokeInfo, info5: InvokeInfo): Unit = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3, info4, info5)
        invokeMultiAutoReply(f, infos)
    }
    def invoke(f: () => Unit, infos: List[InvokeInfo]): Unit = {
        val buff = new ArrayBuffer[InvokeInfo]()
        infos.foreach(info => buff += info)
        invokeMulti(f, buff)
    }
    def invokeAutoReply(f: () => Unit, infos: List[InvokeInfo]): Unit = {
        val buff = new ArrayBuffer[InvokeInfo]()
        infos.foreach(info => buff += info)
        invokeMultiAutoReply(f, buff)
    }

    def invokeMulti(f: () => Unit, infos: ArrayBuffer[InvokeInfo]): Unit = {

        if (f == null) {
            throw new IllegalArgumentException("invoke callback cannot be empty")
        }

        if (lastf != null) {
            throw new RuntimeException("only one callback function can be used")
        }

        Flow.updateCallStats()

        lastf = f
        replyOnError = false

        Flow.isSyncedInvoke.set(false)
        val finished = send(infos)
        if (finished) {
            lastf = null
            f()
        } else {
            flowFinished = false
        }
    }
    def invokeMultiAutoReply(f: () => Unit, infos: ArrayBuffer[InvokeInfo]): Unit = {

        if (f == null) {
            throw new IllegalArgumentException("invoke callback cannot be empty")
        }

        if (lastf != null) {
            throw new RuntimeException("only one callback function can be used")
        }

        Flow.updateCallStats()

        lastf = f
        replyOnError = true
        replyOnErrorCode = 0
        for (info <- infos) {
            val t = info.params.i("errorCode")
            if (t != 0)
                replyOnErrorCode = t // use the last errorCode
        }

        Flow.isSyncedInvoke.set(false)
        val finished = send(infos)
        if (finished) {
            lastf = null
            if (!doReplyOnError()) {
                f()
            }

        } else {
            flowFinished = false
        }
    }

    def doReplyOnError(): Boolean = {
        if (!replyOnError) return false
        replyOnError = false
        var i = 0
        while (i < lastresultarray.size) {
            val ret = lastresultarray(i)
            if (ret.code != 0) {
                if (!replied) {
                    if (replyOnErrorCode != 0) reply(replyOnErrorCode)
                    else reply(ret.code)
                }
                return true
            }
            i += 1
        }
        false
    }
    def sleep(f: () => Unit, timeout: Int): Unit = {

        if (f == null) {
            throw new IllegalArgumentException("sleep callback cannot be empty")
        }

        if (lastf != null) {
            throw new RuntimeException("only one callback function can be used")
        }

        Flow.updateCallStats()

        lastf = f
        replyOnError = false
        flowFinished = false

        Flow.isSyncedInvoke.set(false)

        lastresultarray = ArrayBuffer.fill[InvokeResult](1)(null)
        val subrequestId = nextRequestId()
        val timeoutId = Flow.router.timeoutId(FlowTimoutType.TYPE_CALL, req.serviceId, req.msgId, subrequestId)
        val timer = Flow.router.newTimer(timeoutId, timeout)
        callTimers.put(subrequestId, timer)
    }

    protected[core] def send(infos: ArrayBuffer[InvokeInfo]): Boolean = {

        lastresultarray = ArrayBuffer.fill[InvokeResult](infos.size)(null)
        subrequestIds = nextRequestIds(infos.size)

        var i = 0
        while (i < infos.size) {

            val info = infos(i)
            val subrequestId = subrequestIds(i)
            val ret = send(info, subrequestId)

            if (ret != null) {
                val idx = Flow.parseIdx(subrequestId)
                idxmap.remove(idx)
                lastresultarray(i) = ret
            } else if (info.timeout <= 0) {
                val idx = Flow.parseIdx(subrequestId)
                idxmap.remove(idx)
                lastresultarray(i) = InvokeResult.success(req.requestId)
            } else {
                val timeoutId = Flow.router.timeoutId(FlowTimoutType.TYPE_CALL, req.serviceId, req.msgId, subrequestId)
                val timer = Flow.router.newTimer(timeoutId, info.timeout)
                callTimers.put(subrequestId, timer)
            }

            i += 1
        }

        val finished = lastresultarray.forall(_ != null)

        finished
    }

    private def send(info: InvokeInfo, subrequestId: String): InvokeResult = {

        val (serviceId, msgId) = Flow.router.serviceNameToId(info.service)

        if (serviceId == 0) {
            log.error("service not found, service=%s".format(info.service))
            // this error will not be recorded in csos log
            return new InvokeResult(subrequestId, ResultCodes.SERVICE_NOT_FOUND, new HashMapStringAny())
        }

        filterInvoke(serviceId, msgId, info.params)

        val (newbody, ec) = Flow.router.encodeRequest(serviceId, msgId, info.params)
        if (ec != 0) {
            //log.error("encode request error, serviceId="+serviceId+", msgId="+msgId)
            // this error will not be recorded in csos log
            return new InvokeResult(subrequestId, ec, new HashMapStringAny())
        }

        var connId = req.connId
        if (info.timeout <= 0) {
            connId = Router.DO_NOT_REPLY
        } else {
            if (connId == Router.DO_NOT_REPLY) connId = "subflow:0"
        }

        var encoding = req.encoding
        if (info.params != null) {
            val s = info.params.s("head.encoding")
            if (s != null && s != "")
                encoding = AvenueCodec.parseEncoding(s)
        }

        val newreq = new Request(
            subrequestId,
            connId,
            req.sequence,
            encoding,
            serviceId,
            msgId,
            req.xhead,
            newbody,
            owner)

        newreq.toAddr = info.toAddr
        newreq.parentServiceId = req.serviceId
        newreq.parentMsgId = req.msgId
        if (info.timeout > 0) {
            newreq.expireTimeout = info.timeout
        }

        val ret = Flow.router.send(newreq)

        if (ret != null)
            ret
        else if (info.timeout <= 0)
            InvokeResult.success(subrequestId)
        else
            null
    }

    def reply(code: Int) {
        replyWithMap(code, new HashMapStringAny())
    }

    def reply(code: Int, params: Tuple2[String, Any]*) {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff, false)
        replyWithMap(code, body)
    }

    private def replyWithMap(code: Int, params: HashMapStringAny): Unit = {

        if (replied) {
            throw new RuntimeException("flow already replied")
        }

        Flow.updateCallStats()

        filterReply(code, params)

        val res = new Response(code, params, req)

        Flow.router.reply(new RequestResponseInfo(req, res, logVars))

        replied = true
    }

    protected[core] def paramsToBody(params: ArrayBuffer[Tuple2[String, Any]], xheadSupport: Boolean = true): HashMapStringAny = {
        val body = new HashMapStringAny()
        for ((key, value) <- params) {

            value match {
                case req: Request =>
                    copy(body, xheadSupport, key, req.body)
                case ir: InvokeResult =>
                    copy(body, xheadSupport, key, ir.res)
                case map: HashMapStringAny =>
                    copy(body, xheadSupport, key, map)
                    if (key != "*")
                        body.put(key, map)
                case _ =>
                    copy(body, xheadSupport, key, value)
            }
        }

        body
    }

    private def checkIsXhead(xheadSupport: Boolean, name: String): Tuple2[String, Boolean] = {
        if (xheadSupport && name.startsWith("xhead.")) {
            val t = (name.substring(6), true)
            t
        } else {
            val t = (name, false)
            t
        }
    }

    private def copy(toMap: HashMapStringAny, xheadSupport: Boolean, name: String, value: Any): Unit = {
        if (value == null) return

        val (key, isXhead) = checkIsXhead(xheadSupport, name.trim)
        if (isXhead) {
            req.xhead.put(key, value)
        } else {
            toMap.put(key, value)
        }
    }

    private def copy(toMap: HashMapStringAny, xheadSupport: Boolean, names: String, fromMap: HashMapStringAny): Unit = {

        if (names == "*") {
            toMap ++= fromMap
            return
        }

        val ss = CachedSplitter.commaSplitter.strToArray(names)
        for (s <- ss) {

            var fromKey = ""
            var toKey = ""

            val p = s.indexOf(":")
            if (p <= 0) {
                toKey = s.trim
                fromKey = null
            } else {
                toKey = s.substring(0, p).trim
                fromKey = s.substring(p + 1).trim
            }

            val (key, isXhead) = checkIsXhead(xheadSupport, toKey)
            if (fromKey == null) fromKey = key
            val v = fromMap.getOrElse(fromKey, null)
            if (v != null) {
                if (isXhead)
                    req.xhead.put(key, v)
                else
                    toMap.put(key, v)
            }
        }

    }

    def lasterrorcode(): Int = {

        if (lastresultarray == null || lastresultarray.size == 0) {
            return 0
        }

        if (lastresultarray.size == 1) {
            return lastresultarray(0).code
        }

        var lastcode = 0
        for (i <- 0 until lastresultarray.size) {
            if (lastresultarray(i).code != 0) {
                lastcode = lastresultarray(i).code
            }
        }

        lastcode
    }

    def allresults(): ArrayBuffer[InvokeResult] = lastresultarray

    def lastresult(): InvokeResult = lastresultarray(0)

    def lastresults2(): Tuple2[InvokeResult, InvokeResult] = {
        (lastresultarray(0), lastresultarray(1))
    }
    def lastresults3(): Tuple3[InvokeResult, InvokeResult, InvokeResult] = {
        (lastresultarray(0), lastresultarray(1), lastresultarray(2))
    }
    def lastresults4(): Tuple4[InvokeResult, InvokeResult, InvokeResult, InvokeResult] = {
        (lastresultarray(0), lastresultarray(1), lastresultarray(2), lastresultarray(3))
    }
    def lastresults5(): Tuple5[InvokeResult, InvokeResult, InvokeResult, InvokeResult, InvokeResult] = {
        (lastresultarray(0), lastresultarray(1), lastresultarray(2), lastresultarray(3), lastresultarray(4))
    }

}

abstract class SyncedFlow extends Flow {

    def syncedInvoke(service: String, timeout: Int, params: Tuple2[String, Any]*): InvokeResult = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, timeout, body)
        val rets = syncedInvokeMulti(ArrayBuffer[InvokeInfo](info))
        rets(0)
    }
    def syncedInvokeWithToAddr(service: String, timeout: Int, toAddr: String, params: Tuple2[String, Any]*): InvokeResult = {
        val buff = new ArrayBuffer[Tuple2[String, Any]]()
        for (t <- params) buff += t
        val body = paramsToBody(buff)
        val info = new InvokeInfo(service, timeout, body, toAddr)
        val rets = syncedInvokeMulti(ArrayBuffer[InvokeInfo](info))
        rets(0)
    }
    def syncedInvoke2(info1: InvokeInfo, info2: InvokeInfo): Tuple2[InvokeResult, InvokeResult] = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2)
        val rets = syncedInvokeMulti(infos)
        (rets(0), rets(1))
    }
    def syncedInvoke3(info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo): Tuple3[InvokeResult, InvokeResult, InvokeResult] = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3)
        val rets = syncedInvokeMulti(infos)
        (rets(0), rets(1), rets(2))
    }
    def syncedInvoke4(info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo, info4: InvokeInfo): Tuple4[InvokeResult, InvokeResult, InvokeResult, InvokeResult] = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3, info4)
        val rets = syncedInvokeMulti(infos)
        (rets(0), rets(1), rets(2), rets(3))
    }
    def syncedInvoke5(info1: InvokeInfo, info2: InvokeInfo, info3: InvokeInfo, info4: InvokeInfo, info5: InvokeInfo): Tuple5[InvokeResult, InvokeResult, InvokeResult, InvokeResult, InvokeResult] = {
        val infos = ArrayBuffer[InvokeInfo](info1, info2, info3, info4, info5)
        val rets = syncedInvokeMulti(infos)
        (rets(0), rets(1), rets(2), rets(3), rets(4))
    }

    def maxTimeout(infos: ArrayBuffer[InvokeInfo]): Int = {
        var timeout = 0
        for (info <- infos) {
            if (info.timeout > timeout) timeout = info.timeout
        }
        timeout
    }

    def syncedInvoke(infos: List[InvokeInfo]): ArrayBuffer[InvokeResult] = {
        val buff = new ArrayBuffer[InvokeInfo]()
        infos.foreach(info => buff += info)
        syncedInvokeMulti(buff)
    }

    def syncedInvokeMulti(infos: ArrayBuffer[InvokeInfo]): ArrayBuffer[InvokeResult] = {

        Flow.updateCallStats()

        Flow.isSyncedInvoke.set(true)
        val finished = send(infos)
        Flow.isSyncedInvoke.set(false)
        if (!finished) {

            lock.lock();

            try {
                val ok = allResultsReceived.await(maxTimeout(infos), TimeUnit.MILLISECONDS)
                if (!ok) {
                    var i = 0
                    while (i < lastresultarray.size) {
                        if (lastresultarray(i) == null) {
                            val idx = Flow.parseIdx(subrequestIds(i))
                            idxmap.remove(idx)
                            lastresultarray(i) = InvokeResult.timeout(subrequestIds(i))
                        }
                        i += 1
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        lastresultarray
    }

}


