package scalabpe.core

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

class QuickTimer(val expireTime: Long, val data: Any, val cancelled: AtomicBoolean, val timeoutFunctionId: Int = 0) {

    def cancel() {
        cancelled.set(true)
    }
}

object QuickTimerEngine {
    val count = new AtomicInteger(1)
}

class QuickTimerEngine(val timeoutFunction: (Any) => Unit, val checkInterval: Int = 100) extends Logging with Dumpable {

    val queueMap = new HashMap[Int, Queue[QuickTimer]]()
    val waitingList = new ConcurrentLinkedQueue[Tuple2[Int, QuickTimer]]()

    val shutdown = new AtomicBoolean()
    val shutdownFinished = new AtomicBoolean()

    val timeoutFunctions = new ArrayBuffer[(Any) => Unit]

    val thread = new Thread(new Runnable() {
        def run() {
            service()
        }
    })

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("threads=1").append(",")
        buff.append("waitingList.size=").append(waitingList.size).append(",")

        for ((t, q) <- queueMap) {
            buff.append("timeout(" + t + ").size=").append(q.size).append(",")
        }

        log.info(buff.toString)

    }

    def init() {

        timeoutFunctions += timeoutFunction
        thread.setName("QuickTimerEngine-" + QuickTimerEngine.count.getAndIncrement())
        thread.start()
    }

    def registerAdditionalTimeoutFunction(tf: (Any) => Unit): Int = {
        timeoutFunctions += tf
        timeoutFunctions.size - 1
    }

    def close() {

        shutdown.set(true)
        thread.interrupt()
        while (!shutdownFinished.get()) {
            Thread.sleep(15)
        }

    }

    def newTimer(timeout: Int, data: Any, timeoutFunctionId: Int = 0): QuickTimer = {
        val expireTime = System.currentTimeMillis + timeout
        val timer = new QuickTimer(expireTime, data, new AtomicBoolean(), timeoutFunctionId)
        val tp = (timeout, timer)
        waitingList.offer(tp)
        timer
    }

    def checkTimeout() {

        while (!waitingList.isEmpty()) {

            val (timeout, timer) = waitingList.poll()

            var queue = queueMap.getOrElse(timeout, null)
            if (queue == null) {
                queue = new Queue[QuickTimer]()
                queueMap.put(timeout, queue)
            }
            queue.enqueue(timer)
        }

        val now = System.currentTimeMillis

        for (queue <- queueMap.values) {

            var finished = false
            while (!finished && queue.size > 0) {
                val first = queue.head

                if (first.cancelled.get()) {
                    queue.dequeue
                } else if (first.expireTime <= now) {

                    if (first.timeoutFunctionId <= 0) {

                        try {
                            timeoutFunction(first.data)
                        } catch {
                            case e: Throwable =>
                                log.error("timer callback function exception e={}", e.getMessage)
                        }
                    } else {

                        if (first.timeoutFunctionId >= timeoutFunctions.size) {
                            log.error("timer timeoutFunctionId not found, timeoutFunctionId={}", first.timeoutFunctionId)
                        } else {
                            val tf = timeoutFunctions(first.timeoutFunctionId)
                            try {
                                tf(first.data)
                            } catch {
                                case e: Throwable =>
                                    log.error("timer callback function exception e={}", e.getMessage)
                            }
                        }

                    }

                    queue.dequeue
                } else {
                    finished = true
                }
            }

        }

    }

    def service() {

        while (!shutdown.get()) {

            try {
                checkTimeout()
            } catch {
                case e: Throwable =>
                    log.error("checkTimeout exception, e={}", e.getMessage)
            }

            try {
                Thread.sleep(checkInterval)
            } catch {
                case e: Throwable =>
                    ;
            }
        }

        shutdownFinished.set(true)
    }

}

