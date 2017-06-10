package scalabpe.plugin

import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.xml.Node

import scalabpe.core.InvokeResult
import scalabpe.core.RequestIdGenerator
import scalabpe.core.Router

class ConnLocalQueueActor(override val router: Router, override val cfgNode: Node)
        extends LocalQueueActor(router, cfgNode) {

    var concurrentNum = 1
    val queueConnNumMap = new ConcurrentHashMap[String, AtomicInteger]()

    init_this

    def init_this() {
        var s = (cfgNode \ "@concurrentNum").text
        if (s != "") concurrentNum = s.toInt
        log.info("concurrentNum=" + concurrentNum)
    }

    def getConcurrentNum(msgId: Int): Int = {
        val tempvalue = msgIdCfgMap.getOrElse(msgId.toString, null)
        if (tempvalue != null) {
            val c = if (tempvalue.concurrentNum <= 0) concurrentNum else tempvalue.concurrentNum
            return c
        } else {
            return concurrentNum
        }
    }

    override def onReceiveResponse(res: InvokeResult) {

        // 相比LocalQueue, 这里会将concurrentNum减1再继续后续处理

        val sendingdata = requestIdMap.remove(res.requestId)
        if (sendingdata == null) return

        val body = jsonToBody(sendingdata.json)
        if (body == null) return

        val msgId = body.i("X-MSGID")
        if (msgId <= 0) {
            log.error("X-MSGID not found or not valid in json " + sendingdata.json)
            return
        }

        val maxSendTimes = getMaxSendTimes(msgId)
        val retryInterval = getRetryInterval(msgId)
        val msgConcurrentNum = getConcurrentNum(msgId)

        if (res.code == 0 || sendingdata.sendCount >= maxSendTimes) {

            if (res.code != 0) {
                log.error("send failed, requestId=" + sendingdata.requestId)
            }

            waitingRunnableList.offer(
                new Runnable() {
                    def run() {

                        val ai = queueConnNumMap.get(sendingdata.queueName)
                        if (ai == null) {
                            log.error("queueConnNumMap not found!!!, queueName=" + sendingdata.queueName)
                        } else {
                            ai.decrementAndGet()
                        }

                        commit(sendingdata.queueName, sendingdata.idx)

                        val lastsendingdata = queuesHasData.getOrElse(sendingdata.queueName, null)
                        if (lastsendingdata != null) {
                            lastsendingdata.reset()
                        }

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
                            val ai = queueConnNumMap.get(sendingdata.queueName)
                            if (ai == null) {
                                log.error("queueConnNumMap not found!!!, queueName=" + sendingdata.queueName)
                            } else {
                                ai.decrementAndGet()
                            }
                            retry(sendingdata)
                        }
                    })
                wakeUpSendThread()

            }
        }, retryInterval)

    }

    override def send(sendingdata: LocalQueueSendingData, generatedRequestId: String = null): Boolean = {

        val body = jsonToBody(sendingdata.json)
        if (body == null) return false

        val msgId = body.i("X-MSGID")
        if (msgId <= 0) {
            log.error("X-MSGID not found or not valid in json " + sendingdata.json)
            return false
        }

        val msgConcurrentNum = getConcurrentNum(msgId)

        var ai = queueConnNumMap.get(sendingdata.queueName)
        if (ai == null) {
            ai = new AtomicInteger(0)
            queueConnNumMap.put(sendingdata.queueName, ai)
        }
        val connNum = ai.incrementAndGet()
        val d = new LocalQueueSendingData(sendingdata.queueName, sendingdata.requestId, sendingdata.idx, sendingdata.json, sendingdata.sendCount)
        d.createTime = sendingdata.createTime
        d.requestId = "LQ" + RequestIdGenerator.nextId()
        requestIdMap.put(d.requestId, d)
        sendingdata.requestId = d.requestId

        val ok = super.send(d, d.requestId)
        if (!ok) {
            ai.decrementAndGet()
            requestIdMap.remove(d.requestId)
            return false
        }

        if (connNum < msgConcurrentNum) {
            sendingdata.reset()
        }

        ok
    }

}


