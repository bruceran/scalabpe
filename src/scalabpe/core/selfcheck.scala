package scalabpe.core

import java.io.StringWriter
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.codec.http.DefaultHttpResponse
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpRequestDecoder
import org.jboss.netty.handler.codec.http.HttpResponseEncoder
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.jboss.netty.handler.codec.http.HttpVersion
import org.jboss.netty.util.ThreadNameDeterminer
import org.jboss.netty.util.ThreadRenamingRunnable

import com.fasterxml.jackson.core.JsonFactory

object SelfCheckServer {
    val count = new AtomicInteger(1)
}

class SelfCheckServer(val port: Int, val router: Router) extends SimpleChannelUpstreamHandler with Logging with Dumpable {

    val pipelineFactory = new SelfCheckPipelineFactory()
    var factory: NioServerSocketChannelFactory = _
    var allChannels: ChannelGroup = _
    var bootstrap: ServerBootstrap = _

    val bossThreadFactory = new NamedThreadFactory("selfcheckboss" + SelfCheckServer.count.getAndIncrement())
    val workThreadFactory = new NamedThreadFactory("selfcheckwork" + SelfCheckServer.count.getAndIncrement())

    var bossExecutor: ThreadPoolExecutor = _
    var workerExecutor: ThreadPoolExecutor = _

    val jsonFactory = new JsonFactory()
    val localIp = IpUtils.localIp()

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")

        log.info(buff.toString)
    }

    def init(): Unit = {

        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
        workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]

        factory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor);

        bootstrap = new ServerBootstrap(factory)
        bootstrap.setPipelineFactory(pipelineFactory)
        bootstrap.setOption("child.tcpNoDelay", true)
        bootstrap.setOption("child.keepAlive", true)

        val channel = bootstrap.bind(new InetSocketAddress(port))
        allChannels = new DefaultChannelGroup("SelfCheckServer")
        allChannels.add(channel)

        log.info("SelfCheckServer started")
    }

    def close(): Unit = {

        if (factory != null) {

            log.info("stopping SelfCheckServer")

            val future = allChannels.close()
            future.awaitUninterruptibly()
            allChannels = null

            factory.releaseExternalResources()
            factory = null
        }

        log.info("SelfCheckServer stopped")
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {

        val request = e.getMessage().asInstanceOf[HttpRequest]
        var uri = request.getUri()

        if (uri.indexOf("?") >= 0) {
            uri = uri.substring(0, uri.indexOf("?"))
        }

        try {
            uri match {

                case "/Dump" =>
                    dump(e.getChannel())
                case "/Dump.do" =>
                    dump(e.getChannel())

                case "/SelfCheck" =>
                    selfcheck(e.getChannel())
                case "/SelfCheck.do" =>
                    selfcheck(e.getChannel())
                case "/QuerySelfCheck" =>
                    selfcheck(e.getChannel())
                case "/QuerySelfCheck.do" =>
                    selfcheck(e.getChannel())

                case "/NotifyChanged" =>
                    refresh(e.getChannel())
                case "/NotifyChanged.do" =>
                    refresh(e.getChannel())
                case _ =>
                    writeResponseWithErrorCode(e.getChannel(), HttpResponseStatus.NOT_FOUND)
            }
        } catch {
            case et: Throwable =>
                log.error("exception in selfcheck", et)
                writeResponseWithErrorCode(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR)
        }

    }

    def dump(ch: Channel) {
        val t1 = System.currentTimeMillis
        dump()
        router.dump()

        val t2 = System.currentTimeMillis
        if (t2 - t1 > 5000)
            log.warn("long time to dump, ts={}", t2 - t1)
        writeResponse(ch, "text/plain; charset=UTF-8", "OK")
    }

    def selfcheck(ch: Channel) {
        val t1 = System.currentTimeMillis
        val buff = router.selfcheck()
        val t2 = System.currentTimeMillis
        if (t2 - t1 > 5000)
            log.warn("long time to selfcheck, ts={}", t2 - t1)
        val retStr = resutlToJson(buff)
        writeResponse(ch, "application/json; charset=UTF-8", retStr)
    }

    def resutlToJson(buff: ArrayBuffer[SelfCheckResult]): String = {

        if (buff.size == 0) return "{}"

        val writer = new StringWriter()

        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        for (result <- buff) {

            jsonGenerator.writeObjectFieldStart(result.name)

            jsonGenerator.writeStringField("msg", result.msg)
            jsonGenerator.writeStringField("ip", localIp)
            jsonGenerator.writeNumberField("timestamp", result.timestamp)
            jsonGenerator.writeNumberField("error_id", result.errorId)
            jsonGenerator.writeNumberField("stat", if (result.hasError) 1 else 0)

            jsonGenerator.writeEndObject()
        }

        jsonGenerator.writeEndObject()
        jsonGenerator.close()

        writer.toString()
    }

    def refresh(ch: Channel) {
        val t1 = System.currentTimeMillis
        router.refresh()
        val t2 = System.currentTimeMillis
        if (t2 - t1 > 5000)
            log.warn("long time to refresh, ts={}", t2 - t1)
        writeResponse(ch, "text/plain; charset=UTF-8", "OK")
    }

    def writeResponseWithErrorCode(ch: Channel, errorCode: HttpResponseStatus) {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, errorCode);
        val buff = ChannelBuffers.copiedBuffer("".getBytes())
        response.setContent(buff)
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8")
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, "0")
        val future = ch.write(response)
        future.addListener(ChannelFutureListener.CLOSE)
    }

    def writeResponse(ch: Channel, contentType: String, content: String) {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        val buff = ChannelBuffers.copiedBuffer(content.getBytes())
        response.setContent(buff)
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buff.readableBytes()))
        val future = ch.write(response)
        future.addListener(ChannelFutureListener.CLOSE)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
        log.error("exceptionCaught e={}", e.getCause())
        e.getChannel.close()
    }

    class SelfCheckPipelineFactory extends Object with ChannelPipelineFactory {

        def getPipeline(): ChannelPipeline = {
            val pipeline = Channels.pipeline()
            pipeline.addLast("decoder", new HttpRequestDecoder())
            pipeline.addLast("encoder", new HttpResponseEncoder())
            pipeline.addLast("handler", SelfCheckServer.this)
            pipeline
        }
    }

}

