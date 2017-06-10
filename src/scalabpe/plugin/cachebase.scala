package scalabpe.plugin

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.collection.mutable.HashMap
import scala.xml.Node

import scalabpe.core.Actor
import scalabpe.core.Closable
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.Router

object RemoteCacheLike {

    val TYPE_UNKNOWN = -1
    val TYPE_ARRAYHASH = 1
    val TYPE_MASTERSLAVE = 2
    val TYPE_CONHASH = 3

}

trait RemoteCacheLike extends CacheLike {
    def get(req: Request): Unit
    def set(req: Request): Unit
    def delete(req: Request): Unit
    def getAndDelete(req: Request): Unit
    def getAndCas(req: Request): Unit
    def incrBy(req: Request): Unit
    def decrBy(req: Request): Unit
    def sget(req: Request): Unit
    def sadd(req: Request): Unit
    def sremove(req: Request): Unit
}

abstract class BaseCacheActor(val router: Router, val cfgNode: Node) extends Actor with Logging with Closable with Dumpable {

    var readThreadNumNode = "CacheThreadNum"
    var writeThreadNumNode = "CacheWriteThreadNum"

    var serviceIds: String = _
    var addrs: List[String] = _
    var cacheType = RemoteCacheLike.TYPE_UNKNOWN
    var cache: RemoteCacheLike = _

    val queueSize = 20000
    var readThreadFactory: ThreadFactory = _
    var readPool: ThreadPoolExecutor = _
    var writeThreadFactory: ThreadFactory = _
    var writePool: ThreadPoolExecutor = _
    var readThreadNum = 1
    var writeThreadNum = 1

    def dump() {

        log.info("--- serviceIds=" + serviceIds)

        val buff = new StringBuilder

        buff.append("readPool.size=").append(readPool.getPoolSize).append(",")
        buff.append("readPool.getQueue.size=").append(readPool.getQueue.size).append(",")
        buff.append("writePool.size=").append(writePool.getPoolSize).append(",")
        buff.append("writePool.getQueue.size=").append(writePool.getQueue.size).append(",")

        log.info(buff.toString)

        cache.asInstanceOf[Dumpable].dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        addrs = (cfgNode \ "ServerAddr").map(_.text).toList

        if (addrs.size == 0) {
            val mcfg = (cfgNode \ "MasterServerAddr").text.toString
            val scfg = (cfgNode \ "SlaveServerAddr").text.toString
            if (mcfg != "" && scfg != "")
                addrs = List(mcfg, scfg)
        }

        if (addrs.size == 0) {
            log.error(getClass.getName + " cache addr not configured, serviceIds={}", serviceIds)
        }

        val conHash = (cfgNode \ "ConHash").text
        val arrayHash = (cfgNode \ "ArrayHash").text
        cacheType = if (conHash == "1") RemoteCacheLike.TYPE_CONHASH else if (arrayHash == "1") RemoteCacheLike.TYPE_ARRAYHASH else RemoteCacheLike.TYPE_UNKNOWN

        var s = (cfgNode \ "@readThreadNum").text
        if (s != "") {
            readThreadNum = s.toInt
        } else {
            s = (router.cfgXml \ readThreadNumNode).text
            if (s != "")
                readThreadNum = s.toInt
        }

        s = (cfgNode \ "@writeThreadNum").text
        if (s != "") {
            writeThreadNum = s.toInt
        } else {
            s = (router.cfgXml \ writeThreadNumNode).text
            if (s != "")
                writeThreadNum = s.toInt
        }

        val firstServiceId = serviceIds.split(",")(0)
        readThreadFactory = new NamedThreadFactory("cacheread" + firstServiceId)
        readPool = new ThreadPoolExecutor(readThreadNum, readThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), readThreadFactory)
        writeThreadFactory = new NamedThreadFactory("cachewrite" + firstServiceId)
        writePool = new ThreadPoolExecutor(writeThreadNum, writeThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), writeThreadFactory)
        readPool.prestartAllCoreThreads()
        writePool.prestartAllCoreThreads()
    }

    def close() {
        val t1 = System.currentTimeMillis

        readPool.shutdown()
        writePool.shutdown()
        readPool.awaitTermination(5, TimeUnit.SECONDS)
        writePool.awaitTermination(5, TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn(getClass.getName + " long time to shutdown pool, ts={}", t2 - t1)
    }

    override def receive(v: Any): Unit = {

        v match {
            case req: Request =>

                // log.info("request received, req={}",req.toString)

                if (req.msgId == CacheLike.MSGID_GET || req.msgId == CacheLike.MSGID_GETANDDELETE || req.msgId == CacheLike.MSGID_SGET) {

                    try {
                        readPool.execute(new Runnable() {
                            def run() {

                                try {
                                    onReceive(req)
                                } catch {
                                    case e: Exception =>
                                        log.error("cache exception req={}", req, e)
                                }

                            }
                        })

                    } catch {
                        case e: RejectedExecutionException =>
                            reply(req, ResultCodes.SERVICE_FULL)
                            log.error("cache read queue is full, serviceIds={}", serviceIds)
                    }

                } else if (req.msgId == CacheLike.MSGID_SET || req.msgId == CacheLike.MSGID_DELETE || req.msgId == CacheLike.MSGID_INCRBY
                    || req.msgId == CacheLike.MSGID_DECRBY || req.msgId == CacheLike.MSGID_GETANDCAS
                    || req.msgId == CacheLike.MSGID_SADD || req.msgId == CacheLike.MSGID_SREMOVE) {

                    try {
                        writePool.execute(new Runnable() {
                            def run() {

                                try {
                                    onReceive(req)
                                } catch {
                                    case e: Exception =>
                                        log.error("cache exception req={}", req, e)
                                }

                            }
                        })
                    } catch {
                        case e: RejectedExecutionException =>
                            reply(req, ResultCodes.SERVICE_FULL)
                            log.error("cache write queue is full, serviceIds={}", serviceIds)
                    }
                } else {

                    log.error("unknown msgid")
                }

            case _ =>

                log.error("unknown msg")
        }

    }

    def onReceive(req: Request): Unit = {

        val now = System.currentTimeMillis
        if (req.receivedTime + req.expireTimeout < now) {
            reply(req, ResultCodes.SERVICE_BUSY)
            log.error("cache is busy, req expired, req={}", req)
            return
        }

        req.msgId match {
            case CacheLike.MSGID_GET =>
                cache.get(req)
            case CacheLike.MSGID_SET =>
                cache.set(req)
            case CacheLike.MSGID_DELETE =>
                cache.delete(req)
            case CacheLike.MSGID_GETANDDELETE =>
                cache.getAndDelete(req)
            case CacheLike.MSGID_GETANDCAS =>
                cache.getAndCas(req)
            case CacheLike.MSGID_INCRBY =>
                cache.incrBy(req)
            case CacheLike.MSGID_DECRBY =>
                cache.decrBy(req)
            case CacheLike.MSGID_SGET =>
                cache.sget(req)
            case CacheLike.MSGID_SADD =>
                cache.sadd(req)
            case CacheLike.MSGID_SREMOVE =>
                cache.sremove(req)
            case _ =>
                log.error("unknown msg")
        }
    }

    def reply(req: Request, code: Int): Unit = {
        reply(req, code, new HashMapStringAny())
    }

    def reply(req: Request, code: Int, params: HashMapStringAny): Unit = {

        val res = new Response(code, params, req)
        router.reply(new RequestResponseInfo(req, res))
    }
}

abstract class BaseCacheClient(
    val cacheTypeCfg: Int,
    val serverAddrs: List[String],
    val serviceIds: String,
    val readThreadNum: Int,
    val writeThreadNum: Int,
    val router: Router,
    val cfgNode: Node)
        extends RemoteCacheLike with Logging {

    var timeout = 3000
    var cacheType: Int = _
    val expMap = HashMap[Int, Int]() // serviceId->exp seconds

    def init() {

        initCacheTlv(serviceIds, router.codecs)

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for (serviceId <- serviceIdArray) {
            val codec = router.codecs.findTlvCodec(serviceId)
            if (codec != null) {
                val tlvType = codec.findTlvType(10000)
                if (tlvType == null) {
                    expMap.put(serviceId, 172800)
                } else {
                    val expTime = if (tlvType.defaultValue == null || tlvType.defaultValue == "") 172800 else tlvType.defaultValue.toInt
                    expMap.put(serviceId, expTime)
                }
            }

        }

        cacheTypeCfg match {

            case RemoteCacheLike.TYPE_UNKNOWN =>
                cacheType = if (serverAddrs.size == 2) RemoteCacheLike.TYPE_MASTERSLAVE else RemoteCacheLike.TYPE_ARRAYHASH
            case RemoteCacheLike.TYPE_MASTERSLAVE =>
                cacheType = if (serverAddrs.size >= 2) RemoteCacheLike.TYPE_MASTERSLAVE else RemoteCacheLike.TYPE_ARRAYHASH
            case RemoteCacheLike.TYPE_CONHASH =>
                cacheType = RemoteCacheLike.TYPE_CONHASH
            case RemoteCacheLike.TYPE_ARRAYHASH =>
                cacheType = RemoteCacheLike.TYPE_ARRAYHASH
        }
    }

    def logKeyValue(action: String, key: String, value: String) {

        if (log.isDebugEnabled()) {
            log.debug(action + ", key={},value={}", key, value)
        }

    }

    def get(req: Request): Unit = {

        val (keyFieldNames, dumm1) = findReqFields(req.serviceId, req.msgId)
        val (dummy2, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }

        try {

            val value = get(key)
            if (value == null) {
                logKeyValue("get", key, value)
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }
            logKeyValue("get", key, value)

            val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, value)

            reply(req, 0, body)

        } catch {
            case e: TimeoutException =>
                reply(req, ResultCodes.CACHE_TIMEOUT)
                log.error("get exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_TIMEOUT, key))
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("get exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def getExpire(req: Request): Int = {
        var exp = expMap.getOrElse(req.serviceId, 0) // 0 never expire
        var v = req.i("exptime", -1)
        if (v == -1) {
            v = req.i("expTime", -1)
        }
        if (v != -1) {
            exp = v
        }
        exp
    }

    def set(req: Request): Unit = {

        val (keyFieldNames, valueFieldNames) = findReqFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }
        val value = parseValue(req, valueFieldNames)
        if (value == null) {
            logKeyValue("set", key, value)
            reply(req, ResultCodes.CACHE_VALUE_EMPTY)
            return
        }

        logKeyValue("set", key, value)

        val exp = getExpire(req)

        try {

            val ok = set(key, exp, value)
            if (ok)
                reply(req, 0)
            else
                reply(req, ResultCodes.CACHE_UPDATEFAILED)
        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("set exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def delete(req: Request): Unit = {

        val (keyFieldNames, dummy) = findReqFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }

        logKeyValue("delete", key, null)

        try {

            val ok = delete(key)
            if (ok)
                reply(req, 0)
            else
                reply(req, ResultCodes.CACHE_UPDATEFAILED)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("delete exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def getAndDelete(req: Request): Unit = {

        val (keyFieldNames, dummy1) = findReqFields(req.serviceId, req.msgId)
        val (dummy2, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }

        try {

            val value = getAndDelete(key)
            if (value == null) {
                logKeyValue("getAndDelete", key, value)

                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }

            logKeyValue("getAndDelete", key, value)

            val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, value)

            reply(req, 0, body)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("getAndDelete exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def getAndCas(req: Request): Unit = {

        val (keyFieldNames, valueFieldNames) = findReqFields(req.serviceId, req.msgId)
        val (dummy, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }
        val values = parseValues(req, valueFieldNames)
        if (values == null || values.length != valueFieldNames.length) {
            reply(req, ResultCodes.CACHE_VALUE_EMPTY)
            return
        }
        val addValueString = genValue(values)

        logKeyValue("getAndCas", key, addValueString)
        val exp = getExpire(req)
        try {

            val newvalue = getAndCas(key, exp, addValueString)
            if (newvalue == null) {
                reply(req, ResultCodes.CACHE_UPDATEFAILED)
                return
            }

            val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, newvalue)

            reply(req, 0, body)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("getAndCas exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }
    }

    def incrBy(req: Request): Unit = {

        val (keyFieldNames, valueFieldNames) = findReqFields(req.serviceId, req.msgId)
        val (dummy, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }
        val value = parseValue(req, valueFieldNames)
        if (value == null) {
            logKeyValue("incrBy", key, value)
            reply(req, ResultCodes.CACHE_VALUE_EMPTY)
            return
        }

        logKeyValue("incrBy", key, value)

        val exp = getExpire(req)

        try {

            val newvalue = incrBy(key, exp, value)
            if (newvalue == null) {
                reply(req, ResultCodes.CACHE_UPDATEFAILED)
                return
            }

            val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, newvalue)
            reply(req, 0, body)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("incrBy exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def decrBy(req: Request): Unit = {

        val (keyFieldNames, valueFieldNames) = findReqFields(req.serviceId, req.msgId)
        val (dummy, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }
        val value = parseValue(req, valueFieldNames)
        if (value == null) {
            logKeyValue("decrBy", key, value)
            reply(req, ResultCodes.CACHE_VALUE_EMPTY)
            return
        }

        logKeyValue("decrBy", key, value)

        val exp = getExpire(req)

        try {

            val newvalue = decrBy(key, exp, value)
            if (newvalue == null) {
                reply(req, ResultCodes.CACHE_UPDATEFAILED)
                return
            }

            val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, newvalue)
            reply(req, 0, body)

        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("decrBy exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def sget(req: Request): Unit = {

        val (keyFieldNames, dumm1) = findReqFields(req.serviceId, req.msgId)
        val (dummy2, valueFieldNamesRes, valueFieldTypesRes) = findResFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }

        try {

            val value = sget(key)
            if (value == null) {
                logKeyValue("sget", key, value)
                reply(req, ResultCodes.CACHE_NOT_FOUND)
                return
            }
            logKeyValue("sget", key, value)

            val body = genResponseBody(valueFieldNamesRes, valueFieldTypesRes, value)

            reply(req, 0, body)

        } catch {
            case e: TimeoutException =>
                reply(req, ResultCodes.CACHE_TIMEOUT)
                log.error("sget exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_TIMEOUT, key))
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("sget exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def sadd(req: Request): Unit = {

        val (keyFieldNames, valueFieldNames) = findReqFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }
        val value = parseValue(req, valueFieldNames)
        if (value == null) {
            logKeyValue("sadd", key, value)
            reply(req, ResultCodes.CACHE_VALUE_EMPTY)
            return
        }

        logKeyValue("sadd", key, value)

        val exp = getExpire(req)

        try {

            val ok = sadd(key, exp, value)
            if (ok)
                reply(req, 0)
            else
                reply(req, ResultCodes.CACHE_UPDATEFAILED)
        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("sadd exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def sremove(req: Request): Unit = {

        val (keyFieldNames, valueFieldNames) = findReqFields(req.serviceId, req.msgId)

        val key = parseKey(req, keyFieldNames)
        if (key == null) {
            reply(req, ResultCodes.CACHE_KEY_EMPTY)
            return
        }
        val value = parseValue(req, valueFieldNames)
        if (value == null) {
            logKeyValue("sremove", key, value)
            reply(req, ResultCodes.CACHE_VALUE_EMPTY)
            return
        }

        logKeyValue("sremove", key, value)

        val exp = getExpire(req)

        try {

            val ok = sremove(key, exp, value)
            if (ok)
                reply(req, 0)
            else
                reply(req, ResultCodes.CACHE_UPDATEFAILED)
        } catch {
            case e: Exception =>
                reply(req, ResultCodes.CACHE_FAILED)
                log.error("sremove exception, e=%s,requestId=%s,code=%d,key=%s".format(e.getMessage, req.requestId, ResultCodes.CACHE_FAILED, key))
        }

    }

    def reply(req: Request, code: Int): Unit = {
        reply(req, code, new HashMapStringAny())
    }

    def reply(req: Request, code: Int, params: HashMapStringAny): Unit = {

        val res = new Response(code, params, req)
        router.reply(new RequestResponseInfo(req, res))
    }

    def get(key: String): String
    def set(key: String, exp: Int, value: String): Boolean
    def delete(key: String): Boolean
    def getAndDelete(key: String): String
    def getAndCas(key: String, exp: Int, addValue: String): String
    def incrBy(key: String, exp: Int, value: String): String
    def decrBy(key: String, exp: Int, value: String): String
    def sget(key: String): String
    def sadd(key: String, exp: Int, value: String): Boolean
    def sremove(key: String, exp: Int, value: String): Boolean
}

