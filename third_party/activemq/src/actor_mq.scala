package scalabpe.plugin

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Date
import java.io.{ File, StringWriter }
import java.text.SimpleDateFormat
import javax.jms._
import scala.xml._
import scala.collection.mutable.{ ArrayBuffer, HashMap, HashSet }

import org.apache.activemq._

import com.fasterxml.jackson.core.JsonFactory

import com.sdo.billing.queue._
import com.sdo.billing.queue.impl._

import scalabpe.core._

/*
"ActiveMQ InactivityMonitor Worker" daemon prio=10 tid=0x08549c00 nid=0x3fdb waiting on condition [0x9c269000]  for each connection
"ActiveMQ Transport: tcp:///10.132.17.201:61616@45078" prio=10 tid=0x088f4c00 nid=0x3fd6 runnable [0x9c3ad000]  for each connection
"ActiveMQ InactivityMonitor WriteCheckTimer" daemon prio=10 tid=0x0864f000 nid=0x3fd9 in Object.wait() [0x9c2ba000] shared for all connections
"ActiveMQ InactivityMonitor ReadCheckTimer" daemon prio=10 tid=0x08748000 nid=0x3fd8 in Object.wait() [0x9c30b000] shared for all connections
"mqsedingthread992" prio=10 tid=0x9c7dd400 nid=0x40f5 waiting on condition [0x9c85c000]  for each service, destination
"mqsedingthread991" prio=10 tid=0x9c7d8c00 nid=0x40f4 waiting on condition [0x9c8ad000]  for each service, destination
"mq991-thread-1" prio=10 tid=0x9c7d2000 nid=0x40f3 waiting on condition [0x9bead000]   for mq config node
"Timer-0" prio=10 tid=0x9c7d3000 nid=0x40f2 in Object.wait() [0x9befe000]  for each persist queue manager
 */

class DestinationCfg(val queueName: String, val persistent: Boolean = true)
class MqConn(val brokerUrl: String, val username: String, val password: String)

object MqActor {
    val localDirs = new HashSet[String]()
}

class MqActor(val router: Router, val cfgNode: Node) extends Actor with Logging with Closable with SelfCheckLike with Dumpable {

    val connReg = """^service=([^ ]+)[ ]+username=([^ ]+)[ ]+password=([^ ]+)$""".r
    val mqClients = new HashMap[Int, MqClient]()

    var serviceIds: String = _

    val threadNum = 1
    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    var persistQueueManager: PersistQueueManagerImpl = _

    val sendThreads = new ArrayBuffer[Thread]()
    val jsonFactory = new JsonFactory()
    val localIp = IpUtils.localIp()

    val hasIOException = new AtomicBoolean()

    var pluginObj: MqSelialize = _

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        buff.append("sendThreads.size=").append(sendThreads.size).append(",")

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

        serviceIds = (cfgNode \ "ServiceId").text

        var localDir = (cfgNode \ "LocalDir").text
        if (localDir == "") {
            localDir = Router.dataDir + File.separator + "activemq"
        }

        val s = (cfgNode \ "@plugin").text.toString
        if (s != "") {

            val plugin = s
            try {

                val obj = Class.forName(plugin).getConstructors()(0).newInstance()
                if (!obj.isInstanceOf[MqSelialize]) {
                    throw new RuntimeException("plugin %s is not MqSelialize".format(plugin))
                }
                pluginObj = obj.asInstanceOf[MqSelialize]

            } catch {
                case e: Exception =>
                    log.error("plugin {} cannot be loaded", plugin)
                    throw e
            }

        }

        if (MqActor.localDirs.contains(localDir)) {
            throw new RuntimeException("Mq localDir cannot be the same, the default is data/mq")
        }
        MqActor.localDirs.add(localDir)
        var dataDir = ""
        if (localDir.startsWith("/")) dataDir = localDir
        else dataDir = router.rootDir + File.separator + localDir
        new File(dataDir).mkdirs()
        persistQueueManager = new PersistQueueManagerImpl()
        persistQueueManager.setDataDir(dataDir)
        persistQueueManager.init()

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory("mq" + firstServiceId)
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for (serviceId <- serviceIdArray) {
            val codec = router.codecs.findTlvCodec(serviceId)

            if (codec == null) {
                throw new RuntimeException("serviceId not found, serviceId=" + serviceId)
            }
        }

        val connStr = (cfgNode \ "Connection").text
        val mqConn = parseConnStr(connStr)

        val destNodes = (cfgNode \ "Destination")
        for (p <- destNodes) {
            val serviceId = (p \ "@serviceId").toString.toInt
            val queueName = (p \ "@queueName").toString
            val persistentStr = (p \ "@persistent").toString.toLowerCase
            val persistent = persistentStr == "true" || persistentStr == "yes" || persistentStr == "t" || persistentStr == "y" || persistentStr == "1"

            val mqClient = new MqClient(mqConn, new DestinationCfg(queueName, persistent))
            mqClients.put(serviceId, mqClient)

            val t = new Thread("mqsedingthread" + serviceId) {
                override def run() {
                    sendData(serviceId)
                }
            }
            t.start()
            sendThreads += t
        }

        log.info("MqActor started {}", serviceIds)
    }

    def parseConnStr(connStr: String): MqConn = {

        connStr match {
            case connReg(brokerUrl, username, password) =>
                val t = new MqConn(brokerUrl, username, password)
                t
            case _ =>
                throw new RuntimeException("connection string is not valid,conn=%s".format(connStr))
        }
    }

    def close() {

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5, TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn("MqActor long time to shutdown pool, ts={}", t2 - t1)

        for (t <- sendThreads) {
            t.interrupt()
            t.join()
        }
        sendThreads.clear()

        for ((serviceId, mqClient) <- mqClients) {
            mqClient.close()
        }
        mqClients.clear()

        if (persistQueueManager != null) {
            persistQueueManager.close()
            persistQueueManager = null
        }

        log.info("MqActor closed {} ", serviceIds)
    }

    override def receive(v: Any): Unit = {
        v match {

            case req: Request =>

                try {

                    pool.execute(new Runnable() {
                        def run() {

                            try {
                                onReceive(req)
                            } catch {
                                case e: Exception =>
                                    log.error("MqActor exception req={}", req, e)
                            }

                        }
                    })

                } catch {
                    case e: RejectedExecutionException =>
                        replyError(ResultCodes.SERVICE_FULL, req)
                        log.error("MqActor queue is full, serviceIds={}", serviceIds)
                }

            case _ =>

                log.error("unknown msg")

        }
    }

    def onReceive(v: Any) {

        v match {

            case req: Request =>

                var s = ""

                if (pluginObj != null)
                    s = pluginObj.selialize(req.serviceId, req.msgId, req.body)
                else
                    s = requestToJson(req)

                try {
                    val queue = persistQueueManager.getQueue(String.valueOf(req.serviceId))
                    queue.put(s)
                    replyOk(req)
                    hasIOException.set(false)
                } catch {
                    case e: Exception =>
                        log.error("cannot save data to local mq data={}", s)
                        replyError(-10242500, req)
                        hasIOException.set(true)
                }

            case _ =>

                log.error("unknown msg")

        }
    }

    def sendData(serviceId: Int) {

        val queue = persistQueueManager.getQueue(String.valueOf(serviceId))
        if (queue == null) return

        while (true) {

            try {
                val idx = queue.get()
                if (idx == -1) {
                    return
                }
                val str = queue.getString(idx)

                if (log.isDebugEnabled()) {
                    log.debug("json=" + str)
                }

                val mqClient = mqClients.getOrElse(serviceId, null)
                if (mqClient != null) {

                    var ok = false
                    do {
                        ok = mqClient.send(str)
                        if (!ok) {
                            Thread.sleep(5000)
                        }
                    } while (!ok)

                }
                queue.commit(idx)

            } catch {
                case e: InterruptedException =>
                    return
                case e: Exception =>
                    log.error("exception in sending mq data {}", e.getMessage)
                    Thread.sleep(5000)
            }
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

    def requestToJson(req: Request): String = {

        val writer = new StringWriter()

        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        val f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val now = f.format(new Date())

        // four standard mq fields
        jsonGenerator.writeStringField("messageId", req.requestId)
        jsonGenerator.writeStringField("messageSourceIp", localIp)
        jsonGenerator.writeStringField("messageTimestamp", now)
        jsonGenerator.writeStringField("messageType", req.msgId.toString)

        for ((key, value) <- req.body) {

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

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301006

        for ((serviceId, mqClient) <- mqClients) {
            if (mqClient.hasError()) {
                val msg = "mq [" + mqClient.destCfg.queueName + "] has error"
                buff += new SelfCheckResult("SCALABPE.MQ", errorId, true, msg)
            }
        }

        var ioErrorId = 65301007

        if (hasIOException.get()) {
            val msg = "local persistqueue has io error"
            buff += new SelfCheckResult("SCALABPE.IO", ioErrorId, true, msg)
        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.MQ", errorId)
        }

        buff

    }
}

class MqClient(val mqConn: MqConn, val destCfg: DestinationCfg) extends Logging {

    var connectionFactory: ActiveMQConnectionFactory = _

    var connection: Connection = _
    var session: Session = _
    var destination: Destination = _
    var producer: MessageProducer = _

    init

    def init() {

        loadSo

        connectionFactory = new ActiveMQConnectionFactory()
        connectionFactory.setUserName(mqConn.username)
        connectionFactory.setPassword(decrypt(mqConn.password))
        connectionFactory.setBrokerURL(mqConn.brokerUrl)

        log.info("mq client started, brokerUrl=%s".format(mqConn.brokerUrl))
    }

    def loadSo() {

        try {
            System.loadLibrary("sec");
            log.info("library sec loaded")
        } catch {
            case e: Throwable =>
                log.error("cannot load library sec")
        }

    }

    def close() {
        reset()
        log.info("mq client closed, brokerUrl=%s".format(mqConn.brokerUrl))
    }

    def hasError(): Boolean = {
        return (session == null || producer == null)
    }

    def decrypt(pwd: String): String = {
        if (pwd.startsWith("des:")) {
            decryptDes(pwd.substring(4))
        } else if (pwd.startsWith("desx:")) {
            decryptDesX(pwd.substring(5))
        } else if (pwd.startsWith("rsa:")) {
            decryptRsa(pwd.substring(4))
        } else {
            pwd
        }

    }

    @native def decryptDes(s: String): String;
    @native def decryptDesX(s: String): String;
    @native def decryptRsa(s: String): String;

    def reset() {

        if (producer != null) {
            try {
                producer.close()
            } catch {
                case e: Exception =>
                    log.error("producer.close error brokerUrl=%s, exception=%s".format(mqConn.brokerUrl, e.getMessage))
            }
            producer = null
        }

        destination = null

        if (session != null) {
            try {
                session.close()
            } catch {
                case e: Exception =>
                    log.error("session.close error brokerUrl=%s, exception=%s".format(mqConn.brokerUrl, e.getMessage))
            }
            session = null
        }
        if (connection != null) {
            try {
                connection.close()
            } catch {
                case e: Exception =>
                    log.error("connection.close error brokerUrl=%s, exception=%s".format(mqConn.brokerUrl, e.getMessage))
            }
            connection = null
        }
    }

    def prepare() {
        connection = connectionFactory.createConnection()
        connection.start()
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        destination = session.createQueue(destCfg.queueName)
        producer = session.createProducer(destination)
        val d = if (destCfg.persistent) DeliveryMode.PERSISTENT else DeliveryMode.NON_PERSISTENT
        producer.setDeliveryMode(d)
    }

    def send(str: String): Boolean = {

        try {

            if (session == null || producer == null) {
                prepare()
            }

            val message = session.createTextMessage(str)

            producer.send(message)

            return true

        } catch {
            case e: Exception =>
                log.error("mq send error brokerUrl=%s, exception=%s, str=%s".format(mqConn.brokerUrl, e.getMessage, str))
                reset()
                return false
        }

    }

}

