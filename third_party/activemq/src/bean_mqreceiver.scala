package scalabpe.plugin

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{ TimerTask, Timer }
import java.util.Date
import java.io.{ File, StringWriter }
import javax.jms._
import scala.xml._
import scala.collection.mutable.{ ArrayBuffer, HashMap, HashSet }

import org.apache.activemq._
import org.apache.activemq.transport.TransportListener

import com.fasterxml.jackson.core.JsonFactory

import com.sdo.billing.queue._
import com.sdo.billing.queue.impl._

import scalabpe.core._

object MqReceiverBean {
    val localDirs = new HashSet[String]()
}

class MqReceiverBean(override val router: Router, override val cfgNode: Node)
        extends LocalQueueLike(router, cfgNode) with Bean {

    val connReg = """^service=([^ ]+)[ ]+username=([^ ]+)[ ]+password=([^ ]+)$""".r

    val connCfgList = new ArrayBuffer[MqConn]()
    val destCfgList = new ArrayBuffer[DestinationCfg]()
    var mqReceiver: MqReceiver = _

    var pluginObj: MqDeselialize = _

    init

    override def init() {

        queueTypeName = "mqreceiver"
        serviceIds = "0"

        super.init
        super.afterInit

        val s = (cfgNode \ "@plugin").text.toString
        if (s != "") {

            val plugin = s
            try {

                val obj = Class.forName(plugin).getConstructors()(0).newInstance()
                if (!obj.isInstanceOf[MqDeselialize]) {
                    throw new RuntimeException("plugin %s is not MqDeselialize".format(plugin))
                }
                pluginObj = obj.asInstanceOf[MqDeselialize]

            } catch {
                case e: Exception =>
                    log.error("plugin {} cannot be loaded", plugin)
                    throw e
            }

        }

        val connNodeList = cfgNode \ "Connection"
        for (n <- connNodeList) {
            val connStr = n.text.toString
            val mqConn = parseConnStr(connStr)
            connCfgList += mqConn
        }

        val destNodeList = cfgNode \ "Destination"
        for (p <- destNodeList) {
            val queueName = (p \ "@queueName").toString
            destCfgList += new DestinationCfg(queueName, true)
        }

        mqReceiver = new MqReceiver(connCfgList, destCfgList, this)

        log.info("mq receiver started")
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

    override def checkLocalDir() {

        localDir = (cfgNode \ "LocalDir").text
        if (localDir == "") {
            localDir = Router.dataDir + File.separator + queueTypeName
        }

        if (MqReceiverBean.localDirs.contains(localDir)) {
            throw new RuntimeException("MqReceiverBean localDir cannot be the same, the default is data/" + queueTypeName)
        }

        MqReceiverBean.localDirs.add(localDir)
    }

    override def close() {

        if (mqReceiver != null) {
            mqReceiver.close()
            mqReceiver = null
        }

        super.beforeClose
        super.close

        log.info("mq receiver closed")
    }

    override def dump() {
        super.dump
    }

    override def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = super.selfcheck
        val mqbuff = mqReceiver.selfcheck

        buff ++= mqbuff
        buff
    }

    def processText(queueName: String, txt: String) {

        var body: HashMapStringAny = null
        var msgId = 0

        if (pluginObj != null) {

            body = new HashMapStringAny()
            try {
                msgId = pluginObj.deselialize(queueName, txt, body)
            } catch {
                case e: Throwable =>
                    log.error("deselialize exception", e)
                    return
            }

        }

        if (pluginObj == null || msgId < 0) {

            val json = txt

            body = jsonToBody(json)
            if (body == null) {
                return
            }

            msgId = body.i("messageType")
            if (msgId < 0) {
                log.error("messageType not found in json, json=" + json)
                return
            }

        }

        val newjson = bodyToJson(body, msgId)

        val newQueueName = msgId.toString

        saveToQueue(newQueueName, newjson)
    }

}

class MqReceiverTransportListenerImpl(val res: MqReceiverResource, val mqReceiver: MqReceiver) extends TransportListener with Logging {

    def onCommand(command: Object) {
        /*
        if(log.isDebugEnabled() ) {
            log.debug("command="+command)
        }
         */
    }

    def onException(e: java.io.IOException) {
        log.error("TransportListener exception brokerUrl=" + res.connCfg.brokerUrl + ",e=" + e.getMessage)
        res.hasError = true
        mqReceiver.errorOccurred()
    }

    def transportInterupted() {
        log.error("transportInterupted brokerUrl=" + res.connCfg.brokerUrl)
        res.hasError = true
        mqReceiver.errorOccurred()
    }

    def transportResumed() {
        log.info("transportResumed brokerUrl=" + res.connCfg.brokerUrl)
    }
}

class MqReceiverResource(
        val connCfg: MqConn,
        val destCfgList: ArrayBuffer[DestinationCfg],
        val connectionFactory: ActiveMQConnectionFactory) {

    var listener: MqReceiverTransportListenerImpl = null

    var hasError: Boolean = true
    var connection: Connection = null
    var session: Session = null
    var consumerList = new ArrayBuffer[MessageConsumer]
}

class MqReceiver(
        val connCfgList: ArrayBuffer[MqConn],
        val destCfgList: ArrayBuffer[DestinationCfg],
        val receiver: MqReceiverBean) extends Logging {

    var resList = new ArrayBuffer[MqReceiverResource]()
    val timer = new Timer("mqreceivertimer")

    init

    def init() {

        loadSo

        for (connCfg <- connCfgList) {

            val connectionFactory = new ActiveMQConnectionFactory()
            connectionFactory.setUserName(connCfg.username)
            connectionFactory.setPassword(decrypt(connCfg.password))
            connectionFactory.setBrokerURL(connCfg.brokerUrl)

            val res = new MqReceiverResource(connCfg, destCfgList, connectionFactory)
            res.listener = new MqReceiverTransportListenerImpl(res, this)
            resList += res
        }

        timer.schedule(new TimerTask() {
            def run() {
                reconnect()
            }
        }, 500)

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

    def close() {

        timer.cancel()

        for (res <- resList) {
            close(res)
        }

    }

    def close(res: MqReceiverResource) {

        for (c <- res.consumerList) {
            try {
                c.close()
            } catch {
                case e: Exception =>
                    log.error("consumer.close error brokerUrl=%s, exception=%s".format(res.connCfg.brokerUrl, e.getMessage))
            }
        }
        res.consumerList.clear()

        if (res.session != null) {
            try {
                res.session.close()
            } catch {
                case e: Exception =>
                    log.error("session.close error brokerUrl=%s, exception=%s".format(res.connCfg.brokerUrl, e.getMessage))
            }
            res.session = null
        }
        if (res.connection != null) {
            try {
                res.connection.close()
            } catch {
                case e: Exception =>
                    log.error("connection.close error brokerUrl=%s, exception=%s".format(res.connCfg.brokerUrl, e.getMessage))
            }
            res.connection = null
        }

        res.hasError = true

        log.info("mq receiver closed, brokerUrl=%s".format(res.connCfg.brokerUrl))
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301006

        for (res <- resList) {
            if (res.connection == null || res.session == null || res.hasError) {
                val msg = "mq [" + res.connCfg.brokerUrl + "] has error"
                buff += new SelfCheckResult("SCALABPE.MQ", errorId, true, msg)
            }
        }

        buff
    }

    def errorOccurred() {

        timer.schedule(new TimerTask() {
            def run() {
                reconnect()
            }
        }, 5000)

    }

    def reconnect() {

        for (res <- resList if res.hasError && res.connection != null) {
            close(res)
        }

        for (res <- resList if res.connection == null) {

            try {
                startRes(res)
            } catch {
                case e: Throwable =>
                    log.error("exception in startRes, e=" + e.getMessage)
                    close(res)
            }
        }

        var allConnected = true
        for (res <- resList if res.connection == null) {
            allConnected = false
        }

        if (!allConnected) {
            timer.schedule(new TimerTask() {
                def run() {
                    reconnect()
                }
            }, 5000)
        }
    }

    def startRes(res: MqReceiverResource) {

        res.connection = res.connectionFactory.createConnection()
        res.connection.asInstanceOf[ActiveMQConnection].addTransportListener(res.listener)
        res.connection.start()

        res.session = res.connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

        for (destCfg <- res.destCfgList) {
            val destination = res.session.createQueue(destCfg.queueName)
            val consumer = res.session.createConsumer(destination)
            val listener = new MessageListener() {
                def onMessage(message: javax.jms.Message) {
                    MqReceiver.this.onMessage(destCfg.queueName, message)
                }
            }

            consumer.setMessageListener(listener);
            res.consumerList += consumer
        }

        res.hasError = false

        log.info("mq receiver started, brokerUrl=%s".format(res.connCfg.brokerUrl))
    }

    def onMessage(queueName: String, message: javax.jms.Message) {

        try {
            message match {
                case txtMsg: TextMessage =>
                    val txt = txtMsg.getText()

                    if (log.isDebugEnabled()) {
                        log.debug("txt=" + txt)
                    }

                    processText(queueName, txt)
                case _ =>
                    log.error("unknown message type received")
            }

        } catch {
            case e: JMSException =>
                log.error("exception in processText, e=" + e.getMessage, e)
            case e: Throwable =>
                log.error("exception in processText, e=" + e.getMessage, e)
        }

    }

    def processText(queueName: String, txt: String) {
        receiver.processText(queueName, txt)
    }

}

