package scalabpe.plugin

import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.xml.Node

import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.JobExecutionContext
import org.quartz.Scheduler
import org.quartz.SimpleScheduleBuilder
import org.quartz.TriggerBuilder
import org.quartz.impl.StdSchedulerFactory
import org.quartz.impl.triggers.CronTriggerImpl

import scalabpe.core.Actor
import scalabpe.core.ArrayBufferString
import scalabpe.core.Bean
import scalabpe.core.Closable
import scalabpe.core.DummyActor
import scalabpe.core.HashMapStringAny
import scalabpe.core.InitBean
import scalabpe.core.InvokeResult
import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.RequestIdGenerator
import scalabpe.core.Router

object QuartzBeanJob {
    val dummyActor = new DummyActor()
    var router: Router = null
    var sequence = new AtomicInteger(1)

    def generateSequence(): Int = {
        sequence.getAndIncrement()
    }
}

class QuartzBeanJob extends Job {

    def execute(context: JobExecutionContext) {
        val data = context.getJobDetail().getJobDataMap()
        val serviceId = data.getString("serviceId").toInt
        val msgId = data.getString("msgId").toInt

        val req = new Request(
            "QZ" + RequestIdGenerator.nextId(),
            "quartz:0",
            QuartzBeanJob.generateSequence(),
            1,
            serviceId,
            msgId,
            new HashMapStringAny(),
            new HashMapStringAny(),
            QuartzBeanJob.dummyActor)

        QuartzBeanJob.router.send(req)

    }
}

class QuartzBean(val router: Router, val cfgNode: Node) extends Logging with Bean with InitBean with Actor with Closable {

    val sampleJob = new QuartzBeanJob()
    var scheduler: Scheduler = null
    var initServiceIdMsgIds = ArrayBufferString()
    var initServiceIdMsgIdsIdx = -1
    var started = new AtomicBoolean(false)
    var jobcount = 1
    val timer = new Timer("quartztimer")

    init

    def isInited(): Boolean = {
        started.get()
    }

    def init() {

        QuartzBeanJob.router = router

        val initList = cfgNode \ "Init"
        for (t <- initList) {
            val serviceId = (t \ "@serviceId").text
            val msgId = (t \ "@msgId").text
            initServiceIdMsgIds += (serviceId + ":" + msgId)
        }

        if (initServiceIdMsgIds.size > 0) {
            log.info("QuartzBean load init data ...")
            loadInitData()
            return
        }

        loadQuartz()
    }

    def loadQuartz() {

        timer.cancel()

        val p = new java.util.Properties()
        p.put("org.quartz.threadPool.threadCount", "1")
        val factory = new StdSchedulerFactory()
        factory.initialize(p)
        scheduler = factory.getScheduler()

        val initList = cfgNode \ "Init"
        for (t <- initList) {
            val serviceId = (t \ "@serviceId").text
            val msgId = (t \ "@msgId").text
            val repeatIntervalStr = (t \ "@repeatInterval").text
            if (repeatIntervalStr != "" && repeatIntervalStr != "0") {
                val repeatInterval = repeatIntervalStr.toInt

                val job = JobBuilder.newJob(sampleJob.getClass).withIdentity("job" + jobcount, "group1").build()
                job.getJobDataMap().put("serviceId", serviceId)
                job.getJobDataMap().put("msgId", msgId)

                val startTime = new java.util.Date(System.currentTimeMillis + repeatInterval * 1000)
                val trigger = TriggerBuilder.newTrigger()
                    .withIdentity("trigger" + jobcount, "group1")
                    .startAt(startTime)
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(repeatInterval).repeatForever()).build();

                scheduler.scheduleJob(job, trigger);

                jobcount += 1
            }
        }

        val repeatList = cfgNode \ "Repeat"
        for (t <- repeatList) {
            val serviceId = (t \ "@serviceId").text
            val msgId = (t \ "@msgId").text
            val startDelay = (t \ "@startDelay").text.toInt
            val repeatInterval = (t \ "@repeatInterval").text.toInt

            val job = JobBuilder.newJob(sampleJob.getClass).withIdentity("job" + jobcount, "group1").build()
            job.getJobDataMap().put("serviceId", serviceId)
            job.getJobDataMap().put("msgId", msgId)

            val startTime = new java.util.Date(System.currentTimeMillis + startDelay * 1000)
            val trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger" + jobcount, "group1")
                .startAt(startTime)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(repeatInterval).repeatForever()).build();

            scheduler.scheduleJob(job, trigger);

            jobcount += 1
        }

        val cronList = cfgNode \ "Cron"
        for (t <- cronList) {
            val serviceId = (t \ "@serviceId").text
            val msgId = (t \ "@msgId").text
            val cron = t.text.toString

            val job = JobBuilder.newJob(sampleJob.getClass).withIdentity("job" + jobcount, "group1").build()
            job.getJobDataMap().put("serviceId", serviceId)
            job.getJobDataMap().put("msgId", msgId)

            val trigger = new CronTriggerImpl()
            trigger.setName("trigger" + jobcount)
            trigger.setGroup("group1")
            trigger.setCronExpression(cron)

            scheduler.scheduleJob(job, trigger);

            jobcount += 1
        }

        scheduler.start()
        started.set(true)

        log.info("QuartzBean started")
    }

    def loadInitData(step: Int = 1): Boolean = {
        initServiceIdMsgIdsIdx += step
        if (initServiceIdMsgIdsIdx < initServiceIdMsgIds.size) {

            val ss = initServiceIdMsgIds(initServiceIdMsgIdsIdx).split(":")
            val serviceId = ss(0).toInt
            val msgId = ss(1).toInt

            val req = new Request(
                "QZ" + RequestIdGenerator.nextId(),
                "quartz:0",
                QuartzBeanJob.generateSequence(),
                1,
                serviceId,
                msgId,
                new HashMapStringAny(),
                new HashMapStringAny(),
                this)

            router.send(req)
            return false
        }

        true
    }

    def receive(v: Any) {
        v match {

            case res: InvokeResult =>
                onReceiveResponse(res)
            case _ =>
                log.error("unknown msg")

        }

    }

    def onReceiveResponse(res: InvokeResult) {
        if (res.code != 0) {

            timer.schedule(new TimerTask() {
                def run() {
                    loadInitData(0)
                }
            }, 10000)

            return
        }

        val finished = loadInitData(1)
        if (finished) {
            log.info("QuartzBean load init data finished")
            loadQuartz()
        }

    }

    def close() {
        scheduler.shutdown()
        log.info("QuartzBean stopped")
    }

}

