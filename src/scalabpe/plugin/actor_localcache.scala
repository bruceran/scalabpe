package scalabpe.plugin

import java.io.File
import java.io.PrintWriter
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.xml.Node

import javax.sql.DataSource
import scalabpe.core.Actor
import scalabpe.core.ArrayBufferString
import scalabpe.core.Closable
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.Refreshable
import scalabpe.core.Request
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.Router
import scalabpe.core.SelfCheckLike
import scalabpe.core.SelfCheckResult
import scalabpe.core.SyncedActor

class LocalCacheActor(val router: Router, val cfgNode: Node)
        extends Actor with Logging with SyncedActor with Refreshable with Closable with SelfCheckLike with Dumpable {

    var serviceIds: String = _
    var localCache: LocalCache = _

    val retmap = new ConcurrentHashMap[String, Response]()
    val timer = new Timer("localacachetimer")

    init

    def dump() {

        log.info("--- serviceIds=" + serviceIds)

        localCache.dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text
        val connString = (cfgNode \ "ConnString").text

        if (connString == "") {
            log.warn("LocalCacheActor using local cache file only {}", serviceIds)
        }

        localCache = new LocalCache(serviceIds, connString, router, this)

        if (connString != "") {
            timer.schedule(new TimerTask() {
                def run() {
                    localCache.refresh()
                }
            }, 300000, 300000)

        }

        log.info("LocalCacheActor started {}", serviceIds)

    }

    def close() {
        timer.cancel()
        localCache.close()

        log.info("LocalCacheActor closed {} ", serviceIds)
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {
        val buff = localCache.selfcheck()
        buff
    }

    def refresh() {
        // use the timer thread to execute the refresh job
        timer.schedule(new TimerTask() {
            def run() {
                localCache.refresh()
            }
        }, 1000)
    }

    override def receive(v: Any): Unit = {

        try {
            onReceive(v)
        } catch {
            case e: Exception =>
                log.error("localcache exception v={}", v, e)
        }
    }

    def onReceive(v: Any) {

        v match {

            case req: Request =>

                //log.info("request received, req={}",req)

                // use the caller's thread to execute

                req.msgId match {
                    case CacheLike.MSGID_GET =>
                        localCache.get(req)
                    case CacheLike.MSGID_GETARRAY =>
                        localCache.getArray(req)
                    case CacheLike.MSGID_GETALL =>
                        localCache.getAll(req)
                    case _ =>
                        log.error("unknown msg")
                }

            case _ =>

                log.error("unknown msg")

        }
    }

    def get(requestId: String): Response = {
        retmap.remove(requestId)
    }

    def put(requestId: String, ret: Response) {
        retmap.put(requestId, ret)
    }

}

class LocalCache(
        val serviceIds: String,
        val connString: String,
        val router: Router,
        val owner: LocalCacheActor) extends DbLike with CacheLike with Dumpable {

    var runDir = router.rootDir

    val sqlMap = HashMap[Int, String]() // serviceId -> sql
    val cache = new ConcurrentHashMap[Int, HashMapStringAny]()

    var masterDataSource: DataSource = _

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("cache.size=").append(cache.size).append(",")

        log.info(buff.toString)
    }

    def close() {
        if (masterDataSource != null) {
            closeDataSource(masterDataSource)
            masterDataSource = null
        }
    }

    def init() {

        if (connString != "") {
            runDir = Router.dataDir
        }

        initCacheTlv(serviceIds, router.codecs)

        if (connString != "") {
            val connStr = parseDbConnStr(connString)
            masterDataSource = createDataSource(connStr.jdbcString, connStr.username, connStr.password)
        }

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for (serviceId <- serviceIdArray) {

            val codec = router.codecs.findTlvCodec(serviceId)

            if (codec == null) {
                throw new RuntimeException("serviceId not found, serviceId=" + serviceId)
            }

            if (codec != null) {

                if (connString != "") {

                    val sql = codec.codecAttributes.getOrElse("sql", null)
                    if (sql != null) {
                        sqlMap.put(serviceId, sql)
                    } else {
                        throw new RuntimeException("sql not defined, serviceId=" + serviceId)
                    }

                }
                val dbOk = refreshFromDb(serviceId)
                if (!dbOk) {
                    val fileOk = refreshFromFile(serviceId)
                    if (!fileOk)
                        throw new RuntimeException("local cache cannot be loaded, serviceId=" + serviceId)
                }
            }

        }

    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301003

        if (masterDataSource != null) {
            if (hasError(masterDataSource)) {
                val msg = "master db [" + DbLike.getUrl(masterDataSource) + "] has error"
                buff += new SelfCheckResult("SCALABPE.LOCALCACHEDB", errorId, true, msg)
            }
        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.LOCALCACHEDB", errorId)
        }

        buff
    }

    def refreshFromDb(serviceId: Int): Boolean = {

        if (connString == "") return false

        val sql = sqlMap.getOrElse(serviceId, null)

        val results = query_db(sql, null, null, ArrayBuffer(masterDataSource), null, 0)

        if (results == null || results.rowCount == -1) {
            return false
        }

        val (keyCount, valueCount) = keyValueCountMap.getOrElse(serviceId, null)

        val lines = new ArrayBufferString()
        val m = new HashMapStringAny()
        for (result <- results.results) {
            if (result.size != keyCount + valueCount)
                throw new RuntimeException("local cache init failed, sql fields is not correct, serviceId=%d,sql=%s".format(serviceId, sql))

            val key = genKey(serviceId, result.slice(0, keyCount))
            val value = genValue(result.slice(keyCount, result.size))

            val existed = m.getOrElse(key, null)
            if (existed == null)
                m.put(key, value)
            else {
                existed match {
                    case s: String =>
                        val as = new ArrayBufferString()
                        as += s
                        as += value
                        m.put(key, as)
                    case as: ArrayBufferString =>
                        as += value
                    case _ =>
                }
            }

            val buff = new StringBuilder()
            var i = 0
            while (i < result.size) {
                if (i > 0)
                    buff.append("\t")
                if (result(i) != null)
                    buff.append(result(i))
                i += 1
            }
            lines += buff.toString()

        }
        cache.put(serviceId, m)

        // overwrite file
        val tmpfile = runDir + "/list_" + serviceId
        val writer = new PrintWriter(new File(tmpfile), "UTF-8")
        for (line <- lines) {
            writer.println(line)
        }
        writer.close()
        return true
    }

    def refreshFromFile(serviceId: Int): Boolean = {

        val tmpfile = runDir + "/list_" + serviceId
        if (!new File(tmpfile).exists)
            return false

        val (keyCount, valueCount) = keyValueCountMap.getOrElse(serviceId, null)

        val m = new HashMapStringAny()

        val lines = Source.fromFile(tmpfile, "UTF-8").getLines.toList
        for (line <- lines) {

            val buff = split(line, "\t")

            if (buff.size >= keyCount + valueCount) {

                val key = genKeyOfStrs(serviceId, buff.slice(0, keyCount))
                val value = genValueOfStrs(buff.slice(keyCount, buff.size))

                val existed = m.getOrElse(key, null)
                if (existed == null)
                    m.put(key, value)
                else {
                    existed match {
                        case s: String =>
                            val as = new ArrayBufferString()
                            as += s
                            as += value
                            m.put(key, as)
                        case as: ArrayBufferString =>
                            as += value
                        case _ =>
                    }
                }

            }
        }

        cache.put(serviceId, m)

        return true
    }

    def refresh() {

        if (connString == "") return

        try {
            val serviceIdArray = serviceIds.split(",").map(_.toInt)
            for (serviceId <- serviceIdArray) {
                refresh(serviceId)
            }
        } catch {
            case e: Throwable =>
                log.error("exception in localcache.refresh serviceIds=" + serviceIds, e)
        }
    }

    def refresh(serviceId: Int) {

        val ok = refreshFromDb(serviceId)
        if (!ok)
            log.error("cannot refresh local cache, serviceId={}", serviceId)
    }

    def get(req: Request): Unit = {

        val (keyFieldNames, dummy1) = findReqFields(req.serviceId, req.msgId)
        val (dummy2, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }

        try {

            val map = cache.get(req.serviceId)
            if (map == null) {
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }

            val value = map.getOrElse(key, null)
            if (value == null) {
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }

            value match {
                case s: String =>
                    val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, s)
                    reply(req, 0, body)
                case as: ArrayBufferString =>
                    val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, as(0))
                    reply(req, 0, body)
                case _ =>
                    reply(req, ResultCodes.CACHE_FAILED)
            }

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
        }

    }

    def getArray(req: Request): Unit = {
        val (keyFieldNames, dummy1) = findReqFields(req.serviceId, req.msgId)
        val (dummy2, resValueArrayNames) = findResArrayFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }

        try {

            val map = cache.get(req.serviceId)
            if (map == null) {
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }

            val value = map.getOrElse(key, null)
            if (value == null) {
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }
            var buff: ArrayBufferString = null

            value match {
                case s: String =>
                    buff = ArrayBufferString(s)
                case as: ArrayBufferString =>
                    buff = as
                case _ =>
                    reply(req, ResultCodes.CACHE_FAILED)
                    return
            }

            val body = new HashMapStringAny()

            for (v <- buff) {
                val valueArray = splitValue(v)
                var i = 0
                for (key <- resValueArrayNames) {

                    if (i < valueArray.length) {
                        var t: ArrayBufferString = body.getOrElse(key, null).asInstanceOf[ArrayBufferString]
                        if (t == null) {
                            t = new ArrayBufferString()
                            body.put(key, t)
                        }
                        t += valueArray(i)
                    }
                    i += 1
                }
            }

            reply(req, 0, body)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
        }
    }

    def getAll(req: Request): Unit = {
        val (resKeyArrayNames, resValueArrayNames) = findResArrayFields(req.serviceId, req.msgId)

        try {

            val map = cache.get(req.serviceId)
            if (map == null) {
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }

            val body = new HashMapStringAny()

            for ((key, value) <- map) {

                val keyArray = splitKeys(req.serviceId, key)
                var i = 0
                for (key <- resKeyArrayNames) {

                    if (i < keyArray.length) {
                        var t: ArrayBufferString = body.getOrElse(key, null).asInstanceOf[ArrayBufferString]
                        if (t == null) {
                            t = new ArrayBufferString()
                            body.put(key, t)
                        }
                        t += keyArray(i)
                    }
                    i += 1
                }

                var buff: ArrayBufferString = null

                value match {
                    case s: String =>
                        buff = ArrayBufferString(s)
                    case as: ArrayBufferString =>
                        buff = as
                    case _ =>
                        reply(req, ResultCodes.CACHE_FAILED)
                        return
                }

                for (v <- buff) {
                    val valueArray = splitValue(v)
                    var i = 0
                    for (key <- resValueArrayNames) {

                        if (i < valueArray.length) {
                            var t: ArrayBufferString = body.getOrElse(key, null).asInstanceOf[ArrayBufferString]
                            if (t == null) {
                                t = new ArrayBufferString()
                                body.put(key, t)
                            }
                            t += valueArray(i)
                        }
                        i += 1
                    }
                }
            }

            reply(req, 0, body)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
        }
    }

    def reply(req: Request, code: Int): Unit = {
        reply(req, code, new HashMapStringAny())
    }

    def reply(req: Request, code: Int, params: HashMapStringAny): Unit = {
        val (newbody, ec) = router.encodeResponse(req.serviceId, req.msgId, code, params)
        var errorCode = code
        if (errorCode == 0 && ec != 0) {
            errorCode = ec
        }

        val res = new Response(errorCode, newbody, req)
        owner.put(res.requestId, res)
    }

}


