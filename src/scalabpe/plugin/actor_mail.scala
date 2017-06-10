package scalabpe.plugin

import java.security.Security
import java.util.Properties
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.xml.Node

import javax.mail.Authenticator
import javax.mail.Message
import javax.mail.PasswordAuthentication
import javax.mail.Session
import javax.mail.Transport
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
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

class MailActor(val router: Router, val cfgNode: Node) extends Actor with Logging with Closable with Dumpable {

    var timeout = 15000
    var connectTimeout = 3000
    var maxThreadNum = 16

    var serviceIds: String = _

    val queueSize = 10000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    init

    def dump() {

        log.info("--- serviceIds=" + serviceIds)

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        log.info(buff.toString)

    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        var s = (cfgNode \ "@timeout").text
        if (s != "") timeout = s.toInt * 1000

        s = (cfgNode \ "@connectTimeout").text
        if (s != "") connectTimeout = s.toInt * 1000

        s = (cfgNode \ "@threadNum").text
        if (s != "") maxThreadNum = s.toInt

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory("mail" + firstServiceId)
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        val p = new com.sun.net.ssl.internal.ssl.Provider()
        if (Security.getProvider(p.getName) == null) {
            Security.addProvider(p)
        }

        log.info("MailActor started {}", serviceIds)
    }

    def close() {

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5, TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 100)
            log.warn("MailActor long time to shutdown pool, ts={}", t2 - t1)

        log.info("MailActor stopped {}", serviceIds)
    }

    override def receive(v: Any): Unit = {

        try {
            pool.execute(new Runnable() {
                def run() {
                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error("MailActor exception v={}", v, e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>

                if (v.isInstanceOf[Request])
                    reply(v.asInstanceOf[Request], ResultCodes.SERVICE_FULL)

                log.error("MailActor queue is full, serviceIds={}", serviceIds)
        }
    }

    def onReceive(v: Any): Unit = {

        v match {

            case req: Request =>

                sendMail(req)

            case _ =>

                log.error("unknown msg")

        }
    }

    def sendMail(req: Request) {

        val charSet = req.s("charSet", "utf-8")
        val contentType = req.s("contentType", "text/html")

        val smtpHost = req.s("smtpHost", "")
        val smtpPort = req.s("smtpPort", "465")
        val smtpAuth = req.s("smtpAuth", "true")
        val smtpUser = req.s("smtpUser", "")
        val smtpPwd = req.s("smtpPwd", "")
        val smtpSsl = req.s("smtpSsl", "true")

        if (smtpHost == "" || smtpAuth == "") {
            reply(req, ResultCodes.SERVICE_INTERNALERROR)
            return
        }
        if (smtpAuth == "true" && (smtpUser == "" || smtpPwd == "")) {
            reply(req, ResultCodes.SERVICE_INTERNALERROR)
            return
        }

        val from = req.s("from", smtpUser)
        val to = req.s("to", "")
        val cc = req.s("cc", "")
        val subject = req.s("subject", "")
        val content = req.s("content", "")

        if (to == "" || subject == "") {
            reply(req, ResultCodes.SERVICE_INTERNALERROR)
            return
        }

        try {
            val props = new Properties();
            props.setProperty("mail.smtp.host", smtpHost);
            props.setProperty("mail.smtp.port", smtpPort);
            props.setProperty("mail.smtp.auth", smtpAuth);

            props.setProperty("mail.smtp.connectiontimeout", connectTimeout.toString);
            props.setProperty("mail.smtp.timeout", timeout.toString);

            if (smtpSsl == "true") {
                props.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
                props.setProperty("mail.smtp.socketFactory.fallback", "false");
                props.setProperty("mail.smtp.socketFactory.port", smtpPort);
            }

            val session = Session.getDefaultInstance(props, new Authenticator() {
                override def getPasswordAuthentication(): PasswordAuthentication = {
                    return new PasswordAuthentication(smtpUser, smtpPwd);

                }
            });

            val msg = new MimeMessage(session);

            msg.setFrom(new InternetAddress(from));

            msg.addRecipients(Message.RecipientType.TO, to) // todo address cannot include Chinese

            if (cc != "") {
                msg.addRecipients(Message.RecipientType.CC, cc)
            }

            val enc = new sun.misc.BASE64Encoder();
            msg.setSubject("=?" + charSet + "?B?" + enc.encode(subject.getBytes(charSet)) + "?=")

            msg.setContent(content.toString(), contentType + ";charset=" + charSet)
            msg.setHeader("X-Mailer", "scalabpe mail plugin");
            msg.setSentDate(new java.util.Date());

            Transport.send(msg)
            reply(req, 0)

        } catch {
            case e: Throwable =>
                reply(req, ResultCodes.SERVICE_INTERNALERROR)
                log.error("failed to send mail, to=" + req.s("to") + ", subject=" + req.s("subject"))
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

