package scalabpe.core

import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
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
import scala.xml.XML

import org.jboss.netty.buffer.DynamicChannelBuffer
import org.jboss.netty.handler.codec.http.DefaultHttpRequest
import org.jboss.netty.handler.codec.http.HttpMethod
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.handler.codec.http.HttpVersion
import org.slf4j.Logger
import org.slf4j.LoggerFactory;

import org.jboss.netty.buffer.ChannelBuffer

class AsyncLogActor(val router: Router) extends Actor with Logging with Closable with Dumpable {

    val rootDir = router.rootDir
    val codecs = router.codecs
    val configFile = rootDir + File.separator + "trigger_config.xml"

    val EMPTY_ARRAYBUFFERSTRING = new ArrayBufferString()
    val EMPTY_ARRAYBUFFERBOOLEAN = new ArrayBuffer[Boolean]()
    val EMPTY_MAP = new HashMapStringAny()

    val localIp = IpUtils.localIp()
    val f_tl = new ThreadLocal[SimpleDateFormat]() {
        override def initialValue(): SimpleDateFormat = {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        }
    }

    val splitter = ",  "
    val httpSplitter = ",   "
    val valueSplitter = "^_^"

    val reportUrl = (router.cfgXml \ "ReportUrl").text
    val detailReportUrl = (router.cfgXml \ "DetailReportUrl").text
    val detailReportServiceId = (router.cfgXml \ "DetailReportServiceId").text
    var detailReportServiceIdSet: HashSet[Int] = _
    val passwordFields = (router.cfgXml \ "AsyncLogPasswordFields").text
    var passwordFieldsSet: HashSet[String] = _
    var asyncLogWithFieldName = true
    var asyncLogArray = 1
    var asyncLogLogId = 0
    var asyncLogMaxParamsLen = 500
    var asyncLogMaxContentLen = 500
    val dispatchMap = new HashMap[String, Tuple2[Int, Int]]()
    var sequence = new AtomicInteger(1)

    val requestLogMap = new ConcurrentHashMap[String, Logger]()
    val csosLogMap = new ConcurrentHashMap[String, Logger]()
    val httpRequestLogMap = new ConcurrentHashMap[String, Logger]()

    var hasConfigFile = false
    val requestLogCfgReq = new HashMap[String, ArrayBufferString]()
    val requestLogCfgRes = new HashMap[String, ArrayBufferString]()
    val requestLogCfgResVarFlag = new HashMap[String, ArrayBuffer[Boolean]]()
    val requestLogIndexFields = new HashMap[String, ArrayBufferString]()

    var maxThreadNum = 1
    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    val timer = new Timer("asynclogstats")

    val reqdts = Array(10, 50, 250, 1000, 3000)
    val sosdts = Array(10, 50, 150, 250, 1000)

    val reqStat = new HashMap[String, Array[Int]]()
    val sosStat = new HashMap[String, Array[Int]]()

    val reqStatsLock = new ReentrantLock(false)
    val sosStatsLock = new ReentrantLock(false)

    val reqStatLog = LoggerFactory.getLogger("scalabpe.ReqStatLog")
    val reqSummaryLog = LoggerFactory.getLogger("scalabpe.ReqSummaryLog")
    val sosStatLog = LoggerFactory.getLogger("scalabpe.SosStatLog")

    val statf_tl = new ThreadLocal[SimpleDateFormat]() {
        override def initialValue(): SimpleDateFormat = {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
        }
    }

    var monitorClient: MonitorHttpClient = _
    val shutdown = new AtomicBoolean()

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        log.info(buff.toString)

        if (monitorClient != null)
            monitorClient.dump()

    }

    def init() {

        if (reportUrl != "" || detailReportUrl != "")
            monitorClient = new MonitorHttpClient(this, reportUrl, detailReportUrl)

        if (detailReportServiceId != "") {

            val ss = detailReportServiceId.split(",")
            detailReportServiceIdSet = new HashSet[Int]()

            for (s <- ss) {
                detailReportServiceIdSet.add(s.toInt)
            }
        }
        if (passwordFields != "") {

            val ss = passwordFields.split(",")
            passwordFieldsSet = new HashSet[String]()

            for (s <- ss) {
                passwordFieldsSet.add(s)
            }
        }

        var s = (router.cfgXml \ "AsyncLogThreadNum").text
        if (s != "") maxThreadNum = s.toInt

        s = (router.cfgXml \ "AsyncLogWithFieldName").text
        if (s != "") {
            if (s == "true" || s == "yes" || s == "t" || s == "y" || s == "1") asyncLogWithFieldName = true
            else asyncLogWithFieldName = false
        }
        s = (router.cfgXml \ "AsyncLogArray").text
        if (s != "") {
            asyncLogArray = s.toInt
            if (asyncLogArray < 1) asyncLogArray = 1
        }

        s = (router.cfgXml \ "AsyncLogLogId").text
        if (s != "") {
            asyncLogLogId = s.toInt
        }

        s = (router.cfgXml \ "AsyncLogMaxParamsLen").text
        if (s != "") {
            asyncLogMaxParamsLen = s.toInt
            if (asyncLogMaxParamsLen < 100) asyncLogMaxParamsLen = 100
        }

        s = (router.cfgXml \ "AsyncLogMaxContentLen").text
        if (s != "") {
            asyncLogMaxContentLen = s.toInt
            if (asyncLogMaxContentLen < 100) asyncLogMaxContentLen = 100
        }

        var defaultTarget = ""
        s = (router.cfgXml \ "AsyncLogDispatch" \ "@defaultTarget").text
        if (s != "") {
            defaultTarget = s
        }

        val dispatchItems = router.cfgXml \ "AsyncLogDispatch" \ "Item"
        for (item <- dispatchItems) {
            val serviceIdStr = (item \ "@serviceId").toString
            val msgIdStr = (item \ "@msgId").toString
            val key = serviceIdStr + ":" + msgIdStr
            var target = (item \ "@target").toString
            if (target == "") target = defaultTarget
            if (target != "" && target != "0") {
                target = target.replace(".", ":")
                val ss = target.split(":")
                dispatchMap.put(key, new Tuple2[Int, Int](ss(0).toInt, ss(1).toInt))
            }
        }

        hasConfigFile = new File(configFile).exists
        if (hasConfigFile) {

            val in = new InputStreamReader(new FileInputStream(configFile), "UTF-8")
            val cfgXml = XML.load(in) //  scala.xml.Elem
            in.close()

            val serviceCfg = (cfgXml \ "service").filter(a => (a \ "@name").toString == "RecordLog")
            val items = serviceCfg \ "item"
            for (item <- items) {
                val serviceIdStr = (item \ "@serviceid").toString
                val msgIdStr = (item \ "@msgid").toString
                val key = serviceIdStr + ":" + msgIdStr
                val bs1 = new ArrayBufferString()
                (item \ "request" \ "field").map(a => (a \ "@name").toString).foreach(a => bs1 += a)
                requestLogCfgReq.put(key, bs1)
                val bs2 = new ArrayBufferString()
                (item \ "response" \ "field").map(a => (a \ "@name").toString).foreach(a => bs2 += a)
                requestLogCfgRes.put(key, bs2)
                val bbf = new ArrayBuffer[Boolean]()
                (item \ "response" \ "field").map(a => if ((a \ "@isproc").toString == "true") true else false).foreach(a => bbf += a)
                requestLogCfgResVarFlag.put(key, bbf)
                val indexStr = (item \ "@index").toString
                val indexFields = new ArrayBufferString()
                if (indexStr != "") {
                    val ss = indexStr.split(",")
                    for (s <- ss) indexFields += s
                    requestLogIndexFields.put(key, indexFields)
                }

            }

        }

        threadFactory = new NamedThreadFactory("asynclog")
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        timer.schedule(new TimerTask() {
            def run() {
                while (!shutdown.get()) {

                    val cal = Calendar.getInstance()
                    val seconds = cal.get(Calendar.SECOND)
                    if (seconds >= 5 && seconds <= 8) {

                        try {
                            timer.scheduleAtFixedRate(new TimerTask() {
                                def run() {
                                    val now = statf_tl.get().format(new Date(System.currentTimeMillis - 60000))
                                    AsyncLogActor.this.receive(Stats(now))
                                }
                            }, 0, 60000)
                        } catch {
                            case e: Throwable =>

                        }
                        return
                    }

                    Thread.sleep(1000)
                }
            }
        }, 0)

        log.info("AsyncLogActor started")
    }

    def close() {

        shutdown.set(true)
        timer.cancel()

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5, TimeUnit.SECONDS)

        writeStats(statf_tl.get().format(new Date(System.currentTimeMillis - 60000)))
        writeStats(statf_tl.get().format(new Date(System.currentTimeMillis)))

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn("AsyncLogActor long time to shutdown pool, ts={}", t2 - t1)

        if (monitorClient != null) {
            monitorClient.close()
            monitorClient = null
        }

        log.info("AsyncLogActor stopped")
    }

    def timeToReqIdx(ts: Int): Int = {
        var i = 0
        while (i < reqdts.size) {
            if (ts <= reqdts(i)) return i
            i += 1
        }
        reqdts.size
    }

    def timeToSosIdx(ts: Int): Int = {
        var i = 0
        while (i < sosdts.size) {
            if (ts <= sosdts(i)) return i
            i += 1
        }
        sosdts.size
    }

    def incSosStat(key: String, ts: Int, code: Int) {

        sosStatsLock.lock();
        try {

            var v = sosStat.getOrElse(key, null)
            if (v == null) {
                v = new Array[Int](sosdts.size + 4)
                sosStat.put(key, v)
            }
            val idx = timeToSosIdx(ts) + 2 // start from 2
            v(idx) += 1

            if (code == 0) v(0) += 1 // succ
            else v(1) += 1 // failed

            if (code == -10242504) v(v.size - 1) += 1 // timeout

        } finally {
            sosStatsLock.unlock();
        }

    }

    def incReqStat(key: String, ts: Int, code: Int) {

        reqStatsLock.lock();
        try {

            var v = reqStat.getOrElse(key, null)
            if (v == null) {
                v = new Array[Int](reqdts.size + 3)
                reqStat.put(key, v)
            }
            val idx = timeToReqIdx(ts) + 2
            v(idx) += 1

            if (code == 0) v(0) += 1
            else v(1) += 1

        } finally {
            reqStatsLock.unlock();
        }
    }

    def writeStats(now: String) {

        // log.info("stats received")

        writeSosStats(now)
        writeReqStats(now)
    }

    def writeSosStats(now: String) {

        var sosMatched = new HashMap[String, Array[Int]]()

        sosStatsLock.lock();
        try {

            for ((key, value) <- sosStat if key.startsWith(now)) {
                sosMatched.put(key, value)
            }

            for ((key, value) <- sosMatched) {
                sosStat.remove(key)
            }

        } finally {
            sosStatsLock.unlock();
        }

        for ((key, value) <- sosMatched) {
            sosStatLog.info(key + ",  " + value.mkString(",  "))
        }

        sendSosToMonitor(sosMatched)
    }

    def writeReqStats(now: String) {

        var reqMatched = new HashMap[String, Array[Int]]()

        reqStatsLock.lock();
        try {

            for ((key, value) <- reqStat if key.startsWith(now)) {
                reqMatched.put(key, value)
            }

            for ((key, value) <- reqMatched) {
                reqStat.remove(key)
            }

        } finally {
            reqStatsLock.unlock();
        }

        var totalNum = 0
        val totals = new Array[Int](3)

        for ((key, value) <- reqMatched) {

            reqStatLog.info(key + ",  " + value.mkString(",  "))

            totals(0) += value(0)
            totals(0) += value(1)
            totals(1) += value(0)
            totals(2) += value(1)
        }

        sendReqToMonitor(reqMatched)

        if (totals(0) > 0) {
            val clients = router.sos.stats()
            reqSummaryLog.info(now + ",  request[" + totals.mkString("/") + "]" + ",  client[" + clients.mkString("/") + "]")

            sendReqSummaryToMonitor(totals, clients)
        }

    }

    def sendSosToMonitor(values: HashMap[String, Array[Int]]) {

        val totals = new HashMap[Int, Array[Int]]

        // merge count of serviceId:msgId into serviceId
        for ((key, value) <- values) {
            val ss = key.split(",  ")
            val serviceId = ss(1).toInt

            var v = totals.getOrElse(serviceId, null)
            if (v == null) {
                v = new Array[Int](value.size)
                totals.put(serviceId, v)
            }

            var i = 0
            while (i < value.size) {
                v(i) += value(i)
                i += 1
            }
        }

        val buff = new StringBuilder()

        for ((serviceId, value) <- totals) {

            val page = serviceId

            val total = value(0) + value(1)

            if (detailReportUrl != "" && (detailReportServiceIdSet == null || detailReportServiceIdSet.contains(serviceId))) {

                buff.append("&action[]=1734|1749|1801,").append(page).append(",").append(total) // 子服务请求数
                buff.append("&action[]=1734|1750|1804,").append(page).append(",").append(value(0)) // 子服务响应成功数
                buff.append("&action[]=1734|1751|1807,").append(page).append(",").append(value(1)) // 子服务响应失败数
                buff.append("&action[]=1734|1752|1810,").append(page).append(",").append(value(2)) // 子服务10ms内响应数
                buff.append("&action[]=1734|1753|1813,").append(page).append(",").append(value(3)) // 子服务50ms内响应数
                buff.append("&action[]=1734|1754|1816,").append(page).append(",").append(value(4)) // 子服务150ms内响应数
                buff.append("&action[]=1734|1755|1819,").append(page).append(",").append(value(5)) // 250ms内响应数
                buff.append("&action[]=1734|1756|1759,").append(page).append(",").append(value(6)) // 1s内响应数
                buff.append("&action[]=1734|1757|1762,").append(page).append(",").append(value(7)) // 子服务other时间段响应数
                buff.append("&action[]=1734|2893|2895,").append(page).append(",").append(value(8)) // 子服务响应超时数
            }

        }

        val s = buff.toString
        if (s.length == 0) return

        callStatPages(s)
    }

    def sendReqToMonitor(values: HashMap[String, Array[Int]]) {

        val buff = new StringBuilder()

        for ((key, value) <- values) {

            val ss = key.split(",  ")
            val serviceId = ss(1)
            val msgId = ss(2)
            val page = serviceId + "_" + msgId

            val total = value(0) + value(1)

            if (detailReportUrl != "" && (detailReportServiceIdSet == null || detailReportServiceIdSet.contains(serviceId.toInt))) {
                buff.append("&action[]=1734|1764|1780,").append(page).append(",").append(total) // 对外业务请求数
                buff.append("&action[]=1734|1765|1783,").append(page).append(",").append(value(0)) // 对外业务响应成功数
                buff.append("&action[]=1734|1766|1786,").append(page).append(",").append(value(1)) // 对外业务响应失败数
                buff.append("&action[]=1734|1767|1789,").append(page).append(",").append(value(2)) // 对外业务10ms内响应数
                buff.append("&action[]=1734|1768|1792,").append(page).append(",").append(value(3)) // 对外业务50ms内响应数
                buff.append("&action[]=1734|1769|1795,").append(page).append(",").append(value(4)) // 对外业务250ms内响应数
                buff.append("&action[]=1734|1770|1798,").append(page).append(",").append(value(5)) // 对外业务1s内响应数
                buff.append("&action[]=1734|1771|1774,").append(page).append(",").append(value(6)) // 对外业务3s内响应数
                buff.append("&action[]=1734|1772|1777,").append(page).append(",").append(value(7)) // 对外业务other时间段响应数
            }
        }

        val s = buff.toString
        if (s.length == 0) return

        callStatPages(s)
    }

    def sendReqSummaryToMonitor(reqs: Array[Int], clients: Array[Int]) {

        val buff = new StringBuilder()

        if (reportUrl != "") {
            buff.append("&action[]=1734|1739|1740|1743").append(",").append(clients(0)) // 间隔内连接数
            buff.append("&action[]=1734|1739|1740|1744").append(",").append(clients(1)) // 间隔内断开连接数
            buff.append("&action[]=1734|1739|1740|1745").append(",").append(clients(2)) // 当前连接数
            buff.append("&action[]=1734|1739|1740|1746").append(",").append(reqs(0)) // 间隔内请求数
            buff.append("&action[]=1734|1739|1740|1747").append(",").append(reqs(1)) // 间隔内成功应答数
            buff.append("&action[]=1734|1739|1740|1748").append(",").append(reqs(2)) // 间隔内失败应答数
        }

        val s = buff.toString
        callStatMoreActions(s)
    }

    def callStatPages(s: String) {
        if (monitorClient != null)
            monitorClient.callStatPages(s)
    }

    def callStatMoreActions(s: String) {
        if (monitorClient != null)
            monitorClient.callStatMoreActions(s)
    }
    def findIndexFields(serviceId: Int, msgId: Int): ArrayBufferString = {
        val s = requestLogIndexFields.getOrElse(serviceId + ":" + msgId, null)
        if (s != null) return s
        val s2 = requestLogIndexFields.getOrElse(serviceId + ":-1", null)
        if (s2 != null) return s2
        EMPTY_ARRAYBUFFERSTRING
    }
    def findReqLogCfg(serviceId: Int, msgId: Int): ArrayBufferString = {
        val s = requestLogCfgReq.getOrElse(serviceId + ":" + msgId, null)
        if (s != null) return s
        EMPTY_ARRAYBUFFERSTRING
    }
    def findResLogCfg(serviceId: Int, msgId: Int): ArrayBufferString = {
        val s = requestLogCfgRes.getOrElse(serviceId + ":" + msgId, null)
        if (s != null) return s
        EMPTY_ARRAYBUFFERSTRING
    }
    def findResLogCfgVarFlag(serviceId: Int, msgId: Int): ArrayBuffer[Boolean] = {
        val s = requestLogCfgResVarFlag.getOrElse(serviceId + ":" + msgId, null)
        if (s != null) return s
        EMPTY_ARRAYBUFFERBOOLEAN
    }

    override def receive(v: Any) {
        try {
            pool.execute(new Runnable() {
                def run() {
                    try {
                        onDispatch(v)
                    } catch {
                        case e: Exception =>
                            log.error("asynclog dispatch exception v={}", v, e)
                    }
                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error("asynclog log exception v={}", v, e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("asynclog queue is full")
        }

    }

    def getRequestLog(serviceId: Int, msgId: Int): Logger = {
        val key = serviceId + "." + msgId
        var log = requestLogMap.get(key, null)
        if (log != null) return log
        val key2 = "scalabpe.RequestLog." + key
        log = LoggerFactory.getLogger(key2)
        requestLogMap.put(key, log)
        log
    }

    def getCsosLog(serviceId: Int, msgId: Int): Logger = {
        val key = serviceId + "." + msgId
        var log = csosLogMap.get(key, null)
        if (log != null) return log
        val key2 = "scalabpe.CsosLog." + key
        log = LoggerFactory.getLogger(key2)
        csosLogMap.put(key, log)
        log
    }
    def getHttpRequestLog(serviceId: Int, msgId: Int): Logger = {
        val key = serviceId + "." + msgId
        var log = httpRequestLogMap.get(key, null)
        if (log != null) return log
        val key2 = "scalabpe.HttpRequestLog." + key
        log = LoggerFactory.getLogger(key2)
        httpRequestLogMap.put(key, log)
        log
    }
    def getHttpAccessLog(): Logger = {
        val key = "access"
        var log = httpRequestLogMap.get(key, null)
        if (log != null) return log
        val key2 = "scalabpe.HttpRequestLog." + key
        log = LoggerFactory.getLogger(key2)
        httpRequestLogMap.put(key, log)
        log
    }

    def parseConnId(connId: String): String = {
        if (connId == null || connId == "") return connId

        val p = connId.lastIndexOf(":")
        if (p != -1)
            connId.substring(0, p)
        else
            ""
    }

    def parseFirstAddr(xhead: HashMapStringAny) = {
        var s = xhead.s(Xhead.KEY_FIRST_ADDR, "")
        if (s == "") s = "0:0"
        val addr = s.split(":")
        addr
    }
    def parseLastAddr(xhead: HashMapStringAny) = {
        var s = xhead.s(Xhead.KEY_LAST_ADDR, "")
        if (s == "") s = "0:0"
        val addr = s.split(":")
        addr
    }

    def getXheadRequestId(req: Request): String = {
        val s = req.xs(Xhead.KEY_UNIQUE_ID, "1")
        s
    }

    def onReceive(v: Any) {

        v match {

            case info: RequestResponseInfo =>

                val now = statf_tl.get().format(new Date(info.res.receivedTime))
                val ts = (info.res.receivedTime - info.req.receivedTime).toInt

                val keyBuff = new StringBuilder()
                keyBuff.append(now).append(",  ")

                val fromSosOrSoc = info.req.sender != null && info.req.sender.isInstanceOf[RawRequestActor]
                if (!fromSosOrSoc) {

                    keyBuff.append(info.req.serviceId)
                    keyBuff.append(",  ")
                    keyBuff.append(info.req.msgId)
                    incSosStat(keyBuff.toString, ts, info.res.code)

                    val log = getCsosLog(info.req.serviceId, info.req.msgId)
                    if (!log.isInfoEnabled) return

                    val buff = new StringBuilder

                    buff.append(info.req.requestId).append(splitter)
                    buff.append(info.req.parentServiceId).append(splitter).append(info.req.parentMsgId).append(splitter)
                    buff.append(info.req.serviceId).append(splitter).append(info.req.msgId).append(splitter)
                    buff.append(info.res.remoteAddr).append(splitter)
                    buff.append(info.req.xhead.ns("uniqueId"))
                    buff.append(splitter).append(splitter)

                    val codec = codecs.findTlvCodec(info.req.serviceId)
                    val serviceNameMsgName = codec.serviceName + "." + codec.msgIdToNameMap.getOrElse(info.req.msgId, "unknown")
                    buff.append(serviceNameMsgName).append(splitter)

                    val keysForReq = codec.msgKeysForReq.getOrElse(info.req.msgId, EMPTY_ARRAYBUFFERSTRING)
                    val keysForRes = codec.msgKeysForRes.getOrElse(info.req.msgId, EMPTY_ARRAYBUFFERSTRING)

                    appendToBuff(keysForReq, info.req.body, buff)

                    buff.append(splitter)

                    appendToBuffForRes(keysForRes, null, info.res.body, info.logVars, buff)

                    buff.append(splitter)

                    buff.append(ts).append(splitter)
                    buff.append(info.res.code)

                    log.info(buff.toString)

                } else {

                    keyBuff.append(info.req.serviceId).append(",  ")
                    keyBuff.append(info.req.msgId).append(",  ")
                    keyBuff.append(parseConnId(info.req.connId))

                    incReqStat(keyBuff.toString, ts, info.res.code)

                    val log = getRequestLog(info.req.serviceId, info.req.msgId)
                    if (!log.isInfoEnabled) return

                    val buff = new StringBuilder
                    val clientInfo = parseFirstAddr(info.req.xhead)
                    val addr = parseLastAddr(info.req.xhead)
                    buff.append(addr(0)).append(splitter).append(addr(1)).append(splitter)
                    buff.append(clientInfo(0)).append(splitter).append(clientInfo(1)).append(splitter)
                    val xappid = info.req.xhead.getOrElse("appId", "0")
                    val xareaid = info.req.xhead.getOrElse("areaId", "0")
                    val xsocId = info.req.xhead.getOrElse("socId", "")
                    buff.append(xappid).append(splitter).append(xareaid).append(splitter).append(xsocId).append(splitter)
                    buff.append(info.req.requestId).append(splitter).append(getXheadRequestId(info.req)).append(splitter)
                    buff.append(info.req.serviceId).append(splitter).append(info.req.msgId).append(splitter)
                    buff.append("").append(splitter)
                    val d = new Date(info.req.receivedTime)
                    buff.append(f_tl.get().format(d)).append(splitter).append(ts).append(splitter)
                    if (asyncLogLogId == 1) {
                        val logId = info.req.xhead.getOrElse("logId", "")
                        buff.append(logId)
                    }
                    buff.append(splitter)
                    if(true) {
                        val codec = codecs.findTlvCodec(info.req.serviceId)
                        val serviceNameMsgName = codec.serviceName + "." + codec.msgIdToNameMap.getOrElse(info.req.msgId, "unknown")
                        buff.append(serviceNameMsgName)
                    }
                    buff.append(splitter)

                    appendIndex(buff, info.req.serviceId, info.req.msgId, info.req.body, info.res.body, info.logVars)

                    val codec = codecs.findTlvCodec(info.req.serviceId)

                    var keysForReq: ArrayBufferString = null
                    var keysForRes: ArrayBufferString = null
                    var keysForResVarFlag: ArrayBuffer[Boolean] = null

                    if (hasConfigFile) keysForReq = findReqLogCfg(info.req.serviceId, info.req.msgId)
                    if (keysForReq == null || keysForReq.size == 0) {
                        keysForReq = codec.msgKeysForReq.getOrElse(info.req.msgId, EMPTY_ARRAYBUFFERSTRING)
                        keysForRes = codec.msgKeysForRes.getOrElse(info.req.msgId, EMPTY_ARRAYBUFFERSTRING)
                        keysForResVarFlag = null
                    } else {
                        keysForRes = findResLogCfg(info.req.serviceId, info.req.msgId)
                        keysForResVarFlag = findResLogCfgVarFlag(info.req.serviceId, info.req.msgId)
                    }

                    appendToBuff(keysForReq, info.req.body, buff)

                    buff.append(splitter)

                    appendToBuffForRes(keysForRes, keysForResVarFlag, info.res.body, info.logVars, buff)

                    buff.append(splitter)

                    buff.append(info.res.code)

                    log.info(buff.toString)
                }

            case rawInfo: RawRequestResponseInfo =>

                val now = statf_tl.get().format(new Date(rawInfo.rawRes.receivedTime))
                val ts = (rawInfo.rawRes.receivedTime - rawInfo.rawReq.receivedTime).toInt

                val keyBuff = new StringBuilder()
                keyBuff.append(now).append(",  ")
                keyBuff.append(rawInfo.rawReq.data.serviceId).append(",  ")
                keyBuff.append(rawInfo.rawReq.data.msgId).append(",  ")
                keyBuff.append(parseConnId(rawInfo.rawReq.connId))

                incReqStat(keyBuff.toString, ts, rawInfo.rawRes.data.code)

                val log = getRequestLog(rawInfo.rawReq.data.serviceId, rawInfo.rawReq.data.msgId)
                if (!log.isInfoEnabled) return

                val req = toReq(rawInfo.rawReq)
                val res = toRes(req, rawInfo.rawRes)

                val buff = new StringBuilder
                val clientInfo = parseFirstAddr(req.xhead)
                val addr = parseLastAddr(req.xhead)
                buff.append(addr(0)).append(splitter).append(addr(1)).append(splitter)
                buff.append(clientInfo(0)).append(splitter).append(clientInfo(1)).append(splitter)
                val xappid = req.xhead.getOrElse("appId", "0")
                val xareaid = req.xhead.getOrElse("areaId", "0")
                val xsocId = req.xhead.getOrElse("socId", "")
                buff.append(xappid).append(splitter).append(xareaid).append(splitter).append(xsocId).append(splitter)
                buff.append(req.requestId).append(splitter).append(getXheadRequestId(req)).append(splitter)
                buff.append(req.serviceId).append(splitter).append(req.msgId).append(splitter)
                buff.append("").append(splitter)
                val d = new Date(req.receivedTime)

                buff.append(f_tl.get().format(d)).append(splitter).append(ts).append(splitter)
                if (asyncLogLogId == 1) {
                    val logId = req.xhead.getOrElse("logId", "")
                    buff.append(logId)
                }
                buff.append(splitter)
                if(true) {
                    val codec = codecs.findTlvCodec(req.serviceId)
                    val serviceNameMsgName = codec.serviceName + "." + codec.msgIdToNameMap.getOrElse(req.msgId, "unknown")
                    buff.append(serviceNameMsgName)
                }
                buff.append(splitter)

                appendIndex(buff, req.serviceId, req.msgId, req.body, res.body, null)

                val codec = codecs.findTlvCodec(req.serviceId)

                var keysForReq: ArrayBufferString = null
                var keysForRes: ArrayBufferString = null

                if (hasConfigFile) keysForReq = findReqLogCfg(req.serviceId, req.msgId)
                if (keysForReq == null || keysForReq.size == 0) {
                    if (codec != null) {
                        keysForReq = codec.msgKeysForReq.getOrElse(req.msgId, EMPTY_ARRAYBUFFERSTRING)
                        keysForRes = codec.msgKeysForRes.getOrElse(req.msgId, EMPTY_ARRAYBUFFERSTRING)
                    } else {
                        keysForReq = EMPTY_ARRAYBUFFERSTRING
                        keysForRes = EMPTY_ARRAYBUFFERSTRING
                    }
                } else {
                    keysForRes = findResLogCfg(req.serviceId, req.msgId)
                }

                appendToBuff(keysForReq, req.body, buff)

                buff.append(splitter)

                appendToBuffForRes(keysForRes, null, res.body, null, buff)

                buff.append(splitter)

                buff.append(res.code)

                log.info(buff.toString)

            case httpInfo: HttpSosRequestResponseInfo =>

                val req = httpInfo.req
                val res = httpInfo.res

                val now = statf_tl.get().format(new Date(res.receivedTime))
                val ts = (res.receivedTime - req.receivedTime).toInt

                val keyBuff = new StringBuilder()
                keyBuff.append(now).append(",  ")

                keyBuff.append(req.serviceId).append(",  ")
                keyBuff.append(req.msgId).append(",  ")
                keyBuff.append(parseConnId(req.connId))

                incReqStat(keyBuff.toString, ts, res.code)

                var params = req.params
                val p = params.indexOf("?")
                var uri = params
                if (p >= 0) {
                    uri = params.substring(0, p)
                    params = params.substring(p + 1)
                } else {
                    params = "-"
                }

                var staticFile = req.xhead.s("staticFile", "0") == "1"

                var contentType = req.xhead.s("contentType", "application/json")
                var content = res.content
                var length = 0
                if (!staticFile) {
                    if (contentType == "application/json" || contentType == "text/plain") {
                        if (params.length > asyncLogMaxParamsLen) params = params.substring(0, asyncLogMaxParamsLen)
                        if (content.length > asyncLogMaxContentLen) content = content.substring(0, asyncLogMaxContentLen)
                        content = content.replace("\n", "")
                        length = content.length
                    } else {
                        if (content.startsWith("raw_content:"))
                            length = content.substring(content.lastIndexOf(":") + 1).toInt
                        else if (content.startsWith("static_file:"))
                            length = content.substring(content.lastIndexOf(":") + 1).toInt
                        else
                            length = content.length
                        content = """{"content":"%s","contentType":"%s"}""".format(content, contentType)
                    }
                } else {
                    length = req.xhead.i("contentLength", 0)
                    content = """{"contentType":"%s","contentLength":%d}""".format(contentType, length)
                }

                val clientIpPort = req.xhead.s("clientIpPort", "0:0")

                if (!staticFile) {
                    val log1 = getHttpRequestLog(req.serviceId, req.msgId)
                    if (log1.isInfoEnabled) {
                        val buff = new StringBuilder
                        buff.append(clientIpPort).append(httpSplitter)
                        buff.append(uri).append(httpSplitter)
                        buff.append(req.serviceId).append(httpSplitter)
                        buff.append(req.msgId).append(httpSplitter)
                        buff.append("-1").append(httpSplitter)
                        buff.append("-1").append(httpSplitter)
                        buff.append(httpSplitter).append(httpSplitter).append(httpSplitter)
                        buff.append(params).append(httpSplitter)
                        buff.append(req.xhead.s("serverIpPort", "0:0")).append(httpSplitter)
                        buff.append(ts).append(httpSplitter)
                        buff.append(res.code).append(httpSplitter)
                        buff.append(content).append(httpSplitter)
                        buff.append("0").append(httpSplitter)
                        buff.append("0").append(httpSplitter)
                        buff.append(req.requestId).append(httpSplitter)
                        buff.append(req.xhead.s("host", "unknown_host"))

                        log1.info(buff.toString)
                    }
                }

                val log2 = getHttpAccessLog()
                if (log2.isInfoEnabled) {

                    val splitter = "\t"
                    val buff = new StringBuilder
                    buff.append(ts).append(splitter) // time_used
                    buff.append(clientIpPort).append(splitter) // client ip
                    buff.append(req.xhead.s("remoteIpPort", "-")).append(splitter) // upstream_addr
                    buff.append(req.xhead.s("method", "-")).append(splitter) // method
                    buff.append(req.xhead.s("host", "-")).append(splitter) // host
                    buff.append(uri).append(splitter) // uri
                    buff.append(params).append(splitter) // query string
                    buff.append(req.xhead.s("httpCode", "-")).append(splitter) // http code
                    buff.append(length).append(splitter) // content length
                    buff.append(req.xhead.s("userAgent", "-")) // user agent

                    log2.info(buff.toString)
                }

            case Stats(now) =>

                writeStats(now)

            case _ =>

                log.error("unknown msg received")

        }
    }

    def appendIndex(buff: StringBuilder, serviceId: Int, msgId: Int, req: HashMapStringAny, res: HashMapStringAny, logVars: HashMapStringAny) {
        var indexFields: ArrayBufferString = null
        if (hasConfigFile) {
            indexFields = findIndexFields(serviceId, msgId)
        }
        if (indexFields == null || indexFields.size == 0) {
            buff.append(splitter)
            buff.append(splitter)
            buff.append(splitter)
        } else {
            var i = 0
            while (i < indexFields.size && i < 3) {
                val key = indexFields(i)
                var value = req.s(key, null)
                if (value == null) value = res.s(key, null)
                if (value == null && logVars != null) value = logVars.s(key, null)
                if (value == null) value = ""
                buff.append(value)
                buff.append(splitter)
                i += 1
            }
            while (i < 3) {
                buff.append(splitter)
                i += 1
            }
        }
    }

    def generateSequence(): Int = {
        sequence.getAndIncrement()
    }

    def onDispatch(v: Any) {

        v match {

            case info: RequestResponseInfo =>

                val serviceId = info.req.serviceId
                val msgId = info.req.msgId
                val key = serviceId + ":" + msgId
                val defaultKey = serviceId + ":*"
                val defaultKey2 = serviceId + ":-1"
                var target = dispatchMap.getOrElse(key, null)
                if (target == null)
                    target = dispatchMap.getOrElse(defaultKey, null)
                if (target == null)
                    target = dispatchMap.getOrElse(defaultKey2, null)
                if (target == null) return
                val (targetServiceId, targetMsgId) = target

                val array = toDispatchArray(info.req, info.res, info.logVars)

                val newreq = new Request(
                    "ASYN" + RequestIdGenerator.nextId(),
                    Router.DO_NOT_REPLY,
                    generateSequence(),
                    1,
                    targetServiceId,
                    targetMsgId,
                    new HashMapStringAny(),
                    HashMapStringAny("serviceId" -> serviceId, "msgId" -> msgId, "kvarray" -> array),
                    this)

                router.send(newreq)

            case rawInfo: RawRequestResponseInfo =>

                val serviceId = rawInfo.rawReq.data.serviceId
                val msgId = rawInfo.rawReq.data.msgId
                val key = serviceId + ":" + msgId
                val defaultKey = serviceId + ":*"
                val defaultKey2 = serviceId + ":-1"
                var target = dispatchMap.getOrElse(key, null)
                if (target == null)
                    target = dispatchMap.getOrElse(defaultKey, null)
                if (target == null)
                    target = dispatchMap.getOrElse(defaultKey2, null)
                if (target == null) return
                val (targetServiceId, targetMsgId) = target

                val req = toReq(rawInfo.rawReq)
                val res = toRes(req, rawInfo.rawRes)
                val array = toDispatchArray(req, res, null)

                val newreq = new Request(
                    "ASYN" + RequestIdGenerator.nextId(),
                    Router.DO_NOT_REPLY,
                    generateSequence(),
                    1,
                    targetServiceId,
                    targetMsgId,
                    new HashMapStringAny(),
                    HashMapStringAny("serviceId" -> serviceId, "msgId" -> msgId, "kvarray" -> array),
                    this)

                router.send(newreq)

            case _ =>

        }

    }

    def toDispatchArray(req: Request, res: Response, logVars: HashMapStringAny): ArrayBufferString = {
        val buff = new ArrayBufferString()
        for ((key, value) <- req.body if value != null) {
            val v = toValue(value)
            if (v != null) {
                val s = "req." + key + "=" + v
                buff += s
            }
        }
        for ((key, value) <- res.body if value != null) {
            val v = toValue(value)
            if (v != null) {
                val s = "res." + key + "=" + v
                buff += s
            }
        }
        buff += "res.code=" + res.code
        if (logVars != null) {
            for ((key, value) <- logVars if value != null) {
                val v = toValue(value)
                if (v != null) {
                    val s = "var." + key + "=" + v
                    buff += s
                }
            }
        }
        buff
    }

    def toValue(v: Any): String = {
        if (v == null) return null

        v match {
            case i: Int              => String.valueOf(i)
            case s: String           => s
            case m: HashMapStringAny => JsonCodec.mkString(m)
            case ai: ArrayBufferInt =>
                if (ai.size == 0) null
                else ai(0).toString
            case al: ArrayBufferLong =>
                if (al.size == 0) null
                else al(0).toString
            case ad: ArrayBufferDouble =>
                if (ad.size == 0) null
                else ad(0).toString
            case as: ArrayBufferString =>
                if (as.size == 0) null
                else as(0)
            case am: ArrayBufferMap =>
                if (am.size == 0) null
                else JsonCodec.mkString(am(0))
            case aa: ArrayBufferAny =>
                if (aa.size == 0) null
                val vv = aa(0)
                toValue(vv)
            case _ => v.toString
        }
    }

    def appendToBuff(keys: ArrayBufferString, body: HashMapStringAny, buff: StringBuilder) {
        var i = 0
        while (i < keys.size) {
            if (i > 0) buff.append(valueSplitter)

            val fieldName = keys(i)
            val v = body.getOrElse(fieldName, "")

            if (asyncLogWithFieldName) {
                buff.append(fieldName).append(":")
            }

            if (v != null) {

                if (passwordFieldsSet != null && passwordFieldsSet.contains(fieldName)) {
                    buff.append("***")
                } else {
                    v match {
                        case i: Int                => buff.append(String.valueOf(i))
                        case s: String             => buff.append(removeCrNl(s))
                        case m: HashMapStringAny   => buff.append(removeCrNl(m.toString))
                        case ai: ArrayBufferInt    => buff.append(trimArrayBufferInt(ai).toString)
                        case al: ArrayBufferLong    => buff.append(trimArrayBufferLong(al).toString)
                        case ad: ArrayBufferDouble    => buff.append(trimArrayBufferDouble(ad).toString)
                        case as: ArrayBufferString => buff.append(removeCrNl(trimArrayBufferString(as).toString))
                        case am: ArrayBufferMap    => buff.append(removeCrNl(trimArrayBufferMap(am).toString))
                        case aa: ArrayBufferAny    => buff.append(removeCrNl(trimArrayBufferAny(aa).toString))
                        case _                     => buff.append(v.toString)
                    }
                }
            }
            i += 1
        }
    }
    def appendToBuffForRes(keys: ArrayBufferString, flags: ArrayBuffer[Boolean], body: HashMapStringAny, logVars: HashMapStringAny, buff: StringBuilder) {

        var i = 0
        while (i < keys.size) {
            if (i > 0) buff.append(valueSplitter)

            val fieldName = keys(i)
            var flag = false
            if (flags != null && flags.length > i) flag = flags(i)

            var v: Any = null
            if (flag) {
                if (logVars != null)
                    v = logVars.getOrElse(fieldName, "")
                else
                    v = ""
            } else {
                v = body.getOrElse(fieldName, "")
            }

            if (asyncLogWithFieldName) {
                buff.append(fieldName).append(":")
            }

            if (v != null) {

                if (passwordFieldsSet != null && passwordFieldsSet.contains(fieldName)) {
                    buff.append("***")
                } else {
                    v match {
                        case i: Int                => buff.append(String.valueOf(i))
                        case s: String             => buff.append(removeCrNl(s))
                        case m: HashMapStringAny   => buff.append(removeCrNl(m.toString))
                        case ai: ArrayBufferInt    => buff.append(trimArrayBufferInt(ai).toString)
                        case al: ArrayBufferLong    => buff.append(trimArrayBufferLong(al).toString)
                        case ad: ArrayBufferDouble    => buff.append(trimArrayBufferDouble(ad).toString)
                        case as: ArrayBufferString => buff.append(removeCrNl(trimArrayBufferString(as).toString))
                        case am: ArrayBufferMap    => buff.append(removeCrNl(trimArrayBufferMap(am).toString))
                        case aa: ArrayBufferAny    => buff.append(removeCrNl(trimArrayBufferAny(aa).toString))
                        case _                     => buff.append(v.toString)
                    }
                }
            }
            i += 1
        }

        if (flags == null && logVars != null) {
            for ((fieldName, v) <- logVars) {
                if (i > 0) buff.append(valueSplitter)

                if (asyncLogWithFieldName) {
                    buff.append(fieldName).append(":")
                }

                if (v != null) {

                    if (passwordFieldsSet != null && passwordFieldsSet.contains(fieldName)) {
                        buff.append("***")
                    } else {
                        v match {
                            case i: Int                => buff.append(String.valueOf(i))
                            case s: String             => buff.append(removeCrNl(s))
                            case m: HashMapStringAny   => buff.append(removeCrNl(m.toString))
                            case ai: ArrayBufferInt    => buff.append(trimArrayBufferInt(ai).toString)
                            case al: ArrayBufferLong    => buff.append(trimArrayBufferLong(al).toString)
                            case ad: ArrayBufferDouble    => buff.append(trimArrayBufferDouble(ad).toString)
                            case as: ArrayBufferString => buff.append(removeCrNl(trimArrayBufferString(as).toString))
                            case am: ArrayBufferMap    => buff.append(removeCrNl(trimArrayBufferMap(am).toString))
                            case aa: ArrayBufferAny    => buff.append(removeCrNl(trimArrayBufferAny(aa).toString))
                            case _                     => buff.append(v.toString)
                        }
                    }
                }
            }
        }
    }

    def removeCrNl(s: String): String = {
        removeSeparator(removeNl(removeCr(s)))
    }
    def removeCr(s: String): String = {
        if (s.indexOf("\r") < 0) return s
        s.replaceAll("\r", "")
    }
    def removeNl(s: String): String = {
        if (s.indexOf("\n") < 0) return s
        s.replaceAll("\n", "")
    }
    def removeSeparator(s: String): String = {
        if (s.indexOf(",  ") < 0) return s
        s.replaceAll(",  ", "")
    }

    def trimArrayBufferInt(a: ArrayBufferInt): ArrayBufferInt = {
        if (a.size <= 1) return a
        val buff = new ArrayBufferInt()
        var i = 0
        while (i < asyncLogArray && i < a.size) {
            buff += a(i)
            i += 1
        }
        buff
    }
    def trimArrayBufferLong(a: ArrayBufferLong): ArrayBufferLong = {
        if (a.size <= 1) return a
        val buff = new ArrayBufferLong()
        var i = 0
        while (i < asyncLogArray && i < a.size) {
            buff += a(i)
            i += 1
        }
        buff
    }
    def trimArrayBufferDouble(a: ArrayBufferDouble): ArrayBufferDouble = {
        if (a.size <= 1) return a
        val buff = new ArrayBufferDouble()
        var i = 0
        while (i < asyncLogArray && i < a.size) {
            buff += a(i)
            i += 1
        }
        buff
    }
    
    def trimArrayBufferAny(ai: ArrayBufferAny): ArrayBufferAny = {
        if (ai.size <= 1) return ai
        val buff = new ArrayBufferAny()
        var i = 0
        while (i < asyncLogArray && i < ai.size) {
            buff += ai(i)
            i += 1
        }
        buff
    }
    def trimArrayBufferString(ai: ArrayBufferString): ArrayBufferString = {
        if (ai.size <= 1) return ai
        val buff = new ArrayBufferString()
        var i = 0
        while (i < asyncLogArray && i < ai.size) {
            buff += ai(i)
            i += 1
        }
        buff
    }
    def trimArrayBufferMap(ai: ArrayBufferMap): ArrayBufferMap = {
        if (ai.size <= 1) return ai
        val buff = new ArrayBufferMap()
        var i = 0
        while (i < asyncLogArray && i < ai.size) {
            buff += ai(i)
            i += 1
        }
        buff
    }

	// TODO test
    def makeCopy(buff: ChannelBuffer): ChannelBuffer = {
        buff.duplicate()
    }

    def toRes(req: Request, rawRes: RawResponse): Response = {

        val tlvCodec = codecs.findTlvCodec(rawRes.data.serviceId)
        if (tlvCodec != null) {
            val copiedBody = makeCopy(rawRes.data.body) // may be used by netty, must make a copy first
            val (body, ec) = tlvCodec.decodeResponse(rawRes.data.msgId, copiedBody, rawRes.data.encoding)
            var errorCode = rawRes.data.code
            if (errorCode == 0 && ec != 0) errorCode = ec
            val res = new Response(errorCode, body, req)
            res
        } else {
            val res = new Response(rawRes.data.code, EMPTY_MAP, req)
            res
        }

    }

    def toReq(rawReq: RawRequest): Request = {

        val requestId = rawReq.requestId

        var xhead = EMPTY_MAP

        try {
            xhead = TlvCodec4Xhead.decode(rawReq.data.serviceId, rawReq.data.xhead)
        } catch {
            case e: Exception =>
                log.error("decode exception in async log", e)
        }

        var body = EMPTY_MAP
        val tlvCodec = codecs.findTlvCodec(rawReq.data.serviceId)
        if (tlvCodec != null) {
            val (b, ec) = tlvCodec.decodeRequest(rawReq.data.msgId, rawReq.data.body, rawReq.data.encoding)
            // ignore ec
            body = b
        }

        val req = new Request(
            requestId,
            rawReq.connId,
            rawReq.data.sequence,
            rawReq.data.encoding,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            xhead,
            body,
            null)

        req.receivedTime = rawReq.receivedTime

        return req

    }

    case class Stats(now: String)

}

class MonitorHttpClient(
        val asyncLogActor: AsyncLogActor,
        val reportUrl: String,
        val detailReportUrl: String,
        val connectTimeout: Int = 5000,
        val timerInterval: Int = 1000) extends HttpClient4Netty with Logging with Dumpable {

    var nettyHttpClient: NettyHttpClient = _
    val generator = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[Int, CacheData]()
    val localIp = IpUtils.localIp()
    val timeout = 10000

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("dataMap.size=").append(dataMap.size).append(",")

        log.info(buff.toString)

        nettyHttpClient.dump
    }

    def init() {

        nettyHttpClient = new NettyHttpClient(this,
            connectTimeout,
            timerInterval)

        log.info("MonitorHttpClient started")
    }

    def close() {
        nettyHttpClient.close()
        log.info("MonitorHttpClient stopped")
    }

    def callStatPages(s: String) {

        if (detailReportUrl == "") return

        val body = "ip=" + localIp + s
        send(detailReportUrl, body)
    }

    def callStatMoreActions(s: String) {

        if (reportUrl == "") return

        val body = "ip=" + localIp + s
        send(reportUrl, body)
    }

    def send(url: String, body: String, sendTimes: Int = 0): Unit = {

        val (host, path) = parseHostPath(url)
        if (host == "" || path == "") {
            return
        }

        var httpReq = generateRequest(host, path, body)
        val sequence = generateSequence()
        dataMap.put(sequence, new CacheData(url, body, sendTimes))
        nettyHttpClient.send(sequence, false, host, httpReq, timeout)
    }

    def retry(data: CacheData) {

        val times = data.sendTimes + 1
        if (times >= 3) return
        val url = data.url
        val body = data.body

        if (!asyncLogActor.shutdown.get()) {

            try {
                asyncLogActor.timer.schedule(new TimerTask() {
                    def run() {
                        send(url, body, times)
                    }
                }, 5000)

            } catch {
                case e: Throwable =>
            }

        }

    }

    def receive(sequence: Int, httpRes: HttpResponse): Unit = {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val ok = parseResult(httpRes)
        if (!ok) retry(saved)
    }

    def networkError(sequence: Int) {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        retry(saved)
    }

    def timeoutError(sequence: Int) {
        dataMap.remove(sequence)
        // do nothing
    }

    def parseHostPath(url: String): Tuple2[String, String] = {
        var v = ("", "")
        val p1 = url.indexOf("//");
        if (p1 < 0) return v
        val p2 = url.indexOf("/", p1 + 2);
        if (p2 < 0) return v
        val host = url.substring(p1 + 2, p2)
        val path = url.substring(p2)
        (host, path)
    }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def generateRequest(host: String, path: String, body: String): HttpRequest = {

        val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.POST, path)

        val buffer = new DynamicChannelBuffer(2048);
        buffer.writeBytes(body.getBytes("ISO-8859-1"))

        if (log.isDebugEnabled()) {
            log.debug("post path={}, content={}", path, body)
        }

        httpReq.setContent(buffer);
        httpReq.setHeader("Host", host) // the host include port already
        httpReq.setHeader("Content-Type", "application/x-www-form-urlencoded")
        httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex)
        httpReq.setHeader("User-Agent", "scalabpe monitor client")
        httpReq.setHeader("Connection", "close")

        httpReq
    }

    def parseResult(httpRes: HttpResponse): Boolean = {

        val status = httpRes.getStatus

        if (status.getCode() != 200) {

            if (log.isDebugEnabled()) {
                log.debug("status code={}", status.getCode())
            }

            return false
        }

        val contentTypeStr = httpRes.getHeader("Content-Type")
        val content = httpRes.getContent()
        val contentStr = content.toString(Charset.forName("ISO-8859-1"))

        if (log.isDebugEnabled()) {
            log.debug("contentType={},contentStr={}", contentTypeStr, contentStr)
        }

        contentStr == "true"
    }

    class CacheData(val url: String, val body: String, val sendTimes: Int)

}


