package scalabpe.plugin.http

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.xml.Node

import scalabpe.core.Actor
import scalabpe.core.Closable
import scalabpe.core.Dumpable
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Router

class HttpClientActor(val router: Router, val cfgNode: Node) extends Actor with Logging with Closable with Dumpable {

    var timeout = 15000
    var connectTimeout = 3000
    var maxThreadNum = 2
    var timerInterval = 100
    var maxContentLength = 1048576

    var serviceIds: String = _

    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    var httpClient: HttpClientImpl = _

    init

    def dump() {

        log.info("--- serviceIds=" + serviceIds)

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        log.info(buff.toString)

        httpClient.dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        var s = (cfgNode \ "Timeout").text
        if (s != "") timeout = s.toInt * 1000
        s = (cfgNode \ "@timeout").text
        if (s != "") timeout = s.toInt * 1000

        s = (cfgNode \ "ConnectTimeout").text
        if (s != "") connectTimeout = s.toInt * 1000
        s = (cfgNode \ "@connectTimeout").text
        if (s != "") connectTimeout = s.toInt * 1000

        s = (cfgNode \ "MaxContentLength").text
        if (s != "") maxContentLength = s.toInt
        s = (cfgNode \ "@maxContentLength").text
        if (s != "") maxContentLength = s.toInt

        s = (cfgNode \ "ThreadNum").text
        if (s != "") maxThreadNum = s.toInt
        s = (cfgNode \ "@threadNum").text
        if (s != "") maxThreadNum = s.toInt

        s = (cfgNode \ "TimerInterval").text
        if (s != "") timerInterval = s.toInt
        s = (cfgNode \ "@timerInterval").text
        if (s != "") timerInterval = s.toInt

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory("httpclient" + firstServiceId)
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        httpClient = new HttpClientImpl(cfgNode, router.codecs, this.receive, connectTimeout, timerInterval, 1, maxContentLength)
        log.info("HttpClientActor started {}", serviceIds)
    }

    def close() {

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5, TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn("HttpClientActor long time to shutdown pool, ts={}", t2 - t1)

        httpClient.close()
        log.info("HttpClientActor stopped {}", serviceIds)
    }

    override def receive(v: Any): Unit = {

        try {
            pool.execute(new Runnable() {
                def run() {
                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error("httpclient exception v={}", v, e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                // ignore the message
                log.error("httpclient queue is full, serviceIds={}", serviceIds)
        }
    }

    def onReceive(v: Any): Unit = {

        v match {

            case req: Request =>

                httpClient.send(req, timeout)

            case reqResInfo: RequestResponseInfo =>

                router.reply(reqResInfo)

            case _ =>

                log.error("unknown msg")

        }
    }

}

