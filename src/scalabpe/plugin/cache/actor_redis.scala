package scalabpe.plugin.cache

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

import RedisType.TYPE_ARRAYHASH
import RedisType.TYPE_CLUSTER
import RedisType.TYPE_CONHASH
import RedisType.TYPE_MASTERSLAVE
import RedisType.TYPE_UNKNOWN
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
import scalabpe.core.SelfCheckLike
import scalabpe.core.SelfCheckResult

class RedisActor(val router: Router, val cfgNode: Node)
        extends Actor with Logging with Closable with SelfCheckLike with Dumpable {

    var timeout = 10000

    var serviceIds: String = _

    val queueSize = 100000
    var maxThreadNum = 2
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    var soc: RedisSocTrait = _

    init

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {
        val buff = soc.selfcheck()
        buff
    }

    def dump() {

        log.info("--- serviceIds=" + serviceIds)

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        log.info(buff.toString)

        soc.dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        var s = (cfgNode \ "@threadNum").text
        if (s != "") maxThreadNum = s.toInt

        s = (cfgNode \ "@timeout").text
        if (s != "") timeout = s.toInt

        val conHash = (cfgNode \ "ConHash").text
        val arrayHash = (cfgNode \ "ArrayHash").text
        val cluster = (cfgNode \ "Cluster").text

        var addrs = (cfgNode \ "ServerAddr").map(_.text).toList.mkString(",")
        val serverAddrs = addrs.split(",")
        var cacheType = if (conHash == "1") TYPE_CONHASH
        else if (arrayHash == "1") TYPE_ARRAYHASH
        else if (cluster == "1") TYPE_CLUSTER
        else TYPE_UNKNOWN

        if (cacheType == TYPE_UNKNOWN) {
            if (serverAddrs.size == 2) cacheType = TYPE_MASTERSLAVE
            else if (serverAddrs.size == 1) cacheType = TYPE_ARRAYHASH
            else cacheType = TYPE_CONHASH
        }

        var connectTimeout = 15000
        s = (cfgNode \ "@connectTimeout").text
        if (s != "") connectTimeout = s.toInt

        var pingInterval = 60000
        s = (cfgNode \ "@pingInterval").text
        if (s != "") pingInterval = s.toInt

        var connSizePerAddr = 4
        s = (cfgNode \ "@connSizePerAddr").text
        if (s != "") connSizePerAddr = s.toInt

        var timerInterval = 100
        s = (cfgNode \ "@timerInterval").text
        if (s != "") timerInterval = s.toInt

        var reconnectInterval = 1
        s = (cfgNode \ "@reconnectInterval").text
        if (s != "") reconnectInterval = s.toInt

        var failOver = true
        s = (cfgNode \ "@failOver").text
        if (s != "") {
            failOver = (s == "true" || s == "yes" || s == "t" || s == "y" || s == "1")
        }

        var maxErrorCount = 50
        s = (cfgNode \ "@errorCountToSwitch").text
        if (s != "") {
            maxErrorCount = s.toInt
        }

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory("redisactor-" + firstServiceId)

        if (cacheType != TYPE_CLUSTER)
            soc = new RedisSoc(addrs, cacheType, this.receive, connectTimeout, pingInterval,
                connSizePerAddr, timerInterval, reconnectInterval,
                failOver, maxErrorCount)
        else
            soc = new RedisSoc4Cluster(addrs, this.receive, connectTimeout, pingInterval,
                timerInterval, reconnectInterval)

        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        log.info("RedisActor started {}", serviceIds)
    }

    def close() {

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5, TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn("RedisActor long time to shutdown pool, ts={}", t2 - t1)

        soc.close()
        log.info("RedisActor stopped {}", serviceIds)
    }

    override def receive(v: Any): Unit = {

        try {
            pool.execute(new Runnable() {
                def run() {
                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error("RedisActor exception v={}", v, e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                if (v.isInstanceOf[Request])
                    reply(v.asInstanceOf[Request], ResultCodes.SERVICE_FULL)
                log.error("RedisActor queue is full, serviceIds={}", serviceIds)
        }
    }

    def onReceive(v: Any): Unit = {

        v match {

            case req: Request =>

                soc.send(req, timeout)

            case reqResInfo: RequestResponseInfo =>

                reply(reqResInfo)

            case _ =>

                log.error("unknown msg")

        }
    }

    def reply(req: Request, code: Int): Unit = {
        reply(req, code, new HashMapStringAny())
    }

    def reply(req: Request, code: Int, params: HashMapStringAny): Unit = {
        val res = new Response(code, params, req)
        reply(new RequestResponseInfo(req, res))
    }

    def reply(reqResInfo: RequestResponseInfo): Unit = {
        router.reply(reqResInfo)
    }

}

