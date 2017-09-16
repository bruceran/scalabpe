package scalabpe.plugin.http

import java.io.File
import java.io.RandomAccessFile
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFactory
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelFutureProgressListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.DefaultFileRegion
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.codec.http.HttpChunkAggregator
import org.jboss.netty.handler.codec.http.HttpContentCompressor
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpRequestDecoder
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.handler.codec.http.HttpResponseEncoder
import org.jboss.netty.handler.stream.ChunkedFile
import org.jboss.netty.handler.stream.ChunkedWriteHandler
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler
import org.jboss.netty.handler.timeout.IdleStateEvent
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.ThreadNameDeterminer
import org.jboss.netty.util.ThreadRenamingRunnable
import org.jboss.netty.util.Timer

import scalabpe.core.Dumpable
import scalabpe.core.Flow
import scalabpe.core.HttpSosRequestResponseInfo
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory

// used by netty
trait HttpServer4Netty {
    def receive(req: HttpRequest, connId: String): Unit;
}

class NettyHttpServerHandler(val nettyHttpServer: NettyHttpServer, val sos: HttpServer4Netty)
        extends IdleStateAwareChannelHandler with Logging {

    val conns = new ConcurrentHashMap[String, Channel]()
    val new_conns = new AtomicInteger(0)
    val new_disconns = new AtomicInteger(0)

    var stopFlag = new AtomicBoolean()

    def close() = { stopFlag.set(true) }

    def stats(): Array[Int] = {
        val a = new_conns.getAndSet(0)
        val b = new_disconns.getAndSet(0)
        Array(a, b, conns.size)
    }

    def write(connId: String, response: HttpResponse, keepAlive: Boolean = false, reqResInfo: HttpSosRequestResponseInfo = null): Boolean = {

        val ch = conns.get(connId)
        if (ch == null) {
            log.error("connection not found, id={}", connId)
            return false;
        }

        if (ch.isOpen) {
            val future = ch.write(response)
            if (!keepAlive)
                future.addListener(ChannelFutureListener.CLOSE)
            if (reqResInfo != null) {
                future.addListener(new ChannelFutureListener() {
                    def operationComplete(future: ChannelFuture) {
                        reqResInfo.res.receivedTime = System.currentTimeMillis
                        Flow.router.asyncLogActor.receive(reqResInfo)
                    }
                });
            }
            return true
        }

        return false
    }

    def writeFile(connId: String, response: HttpResponse, keepAlive: Boolean, f: File, fileLength: Long, range_tpl:Tuple2[Long,Long], reqResInfo: HttpSosRequestResponseInfo = null): Boolean = {

        val ch = conns.get(connId)
        if (ch == null) {
            log.error("connection not found, id={}", connId)
            return false;
        }

        if (ch.isOpen) {
            ch.write(response)
            val future = 
                if( range_tpl._1 != -1 ) { 
                    val len = range_tpl._2 - range_tpl._1 + 1
                    val chunksize = 1000000 // todo
                    val raf = new RandomAccessFile(f,"r")
                    ch.write(new ChunkedFile(raf,range_tpl._1,len,chunksize))
                } else { 
                    ch.write(new ChunkedFile(f))
                }
            if (!keepAlive)
                future.addListener(ChannelFutureListener.CLOSE)
            if (reqResInfo != null) {
                future.addListener(new ChannelFutureListener() {
                    def operationComplete(future: ChannelFuture) {
                        reqResInfo.res.receivedTime = System.currentTimeMillis
                        Flow.router.asyncLogActor.receive(reqResInfo)
                    }
                });
            }
            return true
        }

        return false
    }
    def writeFileZeroCopy(connId: String, response: HttpResponse, keepAlive: Boolean, f: File, fileLength: Long,  range_tpl:Tuple2[Long,Long], reqResInfo: HttpSosRequestResponseInfo = null): Boolean = {

        val ch = conns.get(connId)
        if (ch == null) {
            log.error("connection not found, id={}", connId)
            return false;
        }
        var raf: RandomAccessFile = null
        try {
            raf = new RandomAccessFile(f, "r");
        } catch {
            case e: Throwable =>
                log.error("file open failed, f=" + f)
                return false
        }
        val path = f.getCanonicalPath()

        if (ch.isOpen) {
            ch.write(response)
            val region = 
                if( range_tpl._1 != -1 ) { 
                    val len = range_tpl._2 - range_tpl._1 + 1
                    new DefaultFileRegion(raf.getChannel(), range_tpl._1, len, true );
                } else {
                    new DefaultFileRegion(raf.getChannel(), 0, fileLength);
                }

            val future = ch.write(region);
            future.addListener(new ChannelFutureProgressListener() {
                def operationComplete(future: ChannelFuture) {
                    region.releaseExternalResources();
                    if (reqResInfo != null) {
                        reqResInfo.res.receivedTime = System.currentTimeMillis
                        Flow.router.asyncLogActor.receive(reqResInfo)
                    }
                }

                def operationProgressed(future: ChannelFuture, amount: Long, current: Long, total: Long) {
                    log.info("%s: %d / %d (+%d)".format(path, current, total, amount));
                }
            })
            if (!keepAlive)
                future.addListener(ChannelFutureListener.CLOSE)
            return true
        } else {
            try {
                raf.close()
            } catch {
                case e: Throwable =>
            }
        }

        return false
    }
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {

        val ch = e.getChannel
        if (stopFlag.get()) {
            ch.setReadable(false)
            return
        }

        val request = e.getMessage().asInstanceOf[HttpRequest]
        val connId = ctx.getAttachment().asInstanceOf[String];

        try {
            sos.receive(request, connId);
        } catch {
            case e: Exception =>
                log.error("httpserver decode error, connId=" + connId, e);
        }
    }

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {

        new_conns.incrementAndGet()

        val ch = e.getChannel
        val connId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
        ctx.setAttachment(connId);

        log.info("connection started, id={}", connId)

        conns.put(connId, ch)
        //sos.connected(connId)

        if (stopFlag.get()) {
            ch.close
        }
    }

    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {

        new_disconns.incrementAndGet()

        val connId = ctx.getAttachment().asInstanceOf[String];
        log.info("connection ended, id={}", connId)
        conns.remove(connId)
        //sos.disconnected(connId)
    }

    override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent): Unit = {
        val connId = ctx.getAttachment().asInstanceOf[String];
        log.error("connection timeout, id={}", connId)
        e.getChannel.close()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
        val connId = ctx.getAttachment().asInstanceOf[String];
        log.error("connection exception, id={},msg={}", connId, e.toString)
        e.getChannel.close()
    }

    def parseIpPort(s: String): String = {

        val p = s.indexOf("/")

        if (p >= 0)
            s.substring(p + 1)
        else
            s
    }

}

object NettyHttpServer {
    val count = new AtomicInteger(1)
}

class NettyHttpServer(val sos: HttpServer4Netty,
                      val port: Int,
                      val host: String = "*",
                      val idleTimeoutMillis: Int = 45000,
                      val maxContentLength: Int = 5000000,
                      val maxInitialLineLength: Int = 16000,
                      val maxHeaderSize: Int = 16000,
                      val maxChunkSize: Int = 16000,
                      val uploadDir: String = "webapp/upload",
                      val maxUploadLength: Int = 5000000) extends Logging with Dumpable {

    /*
    Creates a new instance with the default maxInitialLineLength (4096}, maxHeaderSize (8192), and maxChunkSize (8192).
    HttpRequestDecoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize)
    */

    var nettyHttpServerHandler: NettyHttpServerHandler = _
    var allChannels: ChannelGroup = _
    var channelFactory: ChannelFactory = _
    var timer: Timer = _

    val bossThreadFactory = new NamedThreadFactory("httpserverboss" + NettyHttpServer.count.getAndIncrement())
    val workThreadFactory = new NamedThreadFactory("httpserverwork" + NettyHttpServer.count.getAndIncrement())
    val timerThreadFactory = new NamedThreadFactory("httpservertimer" + NettyHttpServer.count.getAndIncrement())

    var bossExecutor: ThreadPoolExecutor = _
    var workerExecutor: ThreadPoolExecutor = _

    def stats(): Array[Int] = {
        nettyHttpServerHandler.stats
    }

    def dump() {
        if (nettyHttpServerHandler == null) return

        val buff = new StringBuilder

        buff.append("nettyHttpServerHandler.conns.size=").append(nettyHttpServerHandler.conns.size).append(",")
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")

        log.info(buff.toString)
    }

    def start(): Unit = {

        nettyHttpServerHandler = new NettyHttpServerHandler(this, sos)

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        timer = new HashedWheelTimer(timerThreadFactory, 1, TimeUnit.SECONDS)

        allChannels = new DefaultChannelGroup("netty-httpserver-scala")

        bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
        workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]
        channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor)

        val bootstrap = new ServerBootstrap(channelFactory)
        bootstrap.setPipelineFactory(new NettyHttpServerPipelineFactory())
        bootstrap.setOption("child.tcpNoDelay", true)
        // bootstrap.setOption("child.keepAlive", true)
        bootstrap.setOption("reuseAddress", true)
        bootstrap.setOption("child.receiveBufferSize", 65536)

        val addr =
            if (host == null || "*" == host) {
                new InetSocketAddress(port)
            } else {
                new InetSocketAddress(host, port)
            }

        val channel = bootstrap.bind(addr)
        allChannels.add(channel)

        val s = "netty httpserver started on host(" + host + ") port(" + port + ")"
        log.info(s)
    }

    def write(connId: String, response: HttpResponse, keepAlive: Boolean = true, reqResInfo: HttpSosRequestResponseInfo = null): Boolean = {
        nettyHttpServerHandler.write(connId, response, keepAlive, reqResInfo)
    }

    def writeFile(connId: String, response: HttpResponse, keepAlive: Boolean, f: File, fileLength: Long, range_tpl:Tuple2[Long,Long], reqResInfo: HttpSosRequestResponseInfo = null): Boolean = {
        nettyHttpServerHandler.writeFile(connId, response, keepAlive, f, fileLength, range_tpl, reqResInfo)
    }

    def writeFileZeroCopy(connId: String, response: HttpResponse, keepAlive: Boolean, f: File, fileLength: Long,  range_tpl:Tuple2[Long,Long], reqResInfo: HttpSosRequestResponseInfo = null): Boolean = {
        nettyHttpServerHandler.writeFileZeroCopy(connId, response, keepAlive, f, fileLength, range_tpl, reqResInfo)
    }

    def closeReadChannel() {
        if (nettyHttpServerHandler != null) {
            nettyHttpServerHandler.close()
        }
        log.info("nettyHttpServerHandler read channel stopped")
    }

    def close(): Unit = {

        if (channelFactory != null) {

            log.info("Stopping netty httpserver")

            timer.stop()
            timer = null

            val chs = nettyHttpServerHandler.conns.values.iterator
            while (chs.hasNext()) {
                allChannels.add(chs.next())
            }
            val future = allChannels.close()
            future.awaitUninterruptibly()
            allChannels = null

            channelFactory.releaseExternalResources()
            channelFactory = null

            log.info("netty httpserver stopped")
        }
    }

    class NettyHttpServerPipelineFactory extends Object with ChannelPipelineFactory {

        def getPipeline(): ChannelPipeline = {
            val pipeline = Channels.pipeline()
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, idleTimeoutMillis / 1000))
            pipeline.addLast("decoder", new HttpRequestDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize))
            //pipeline.addLast("uploader", new HttpFileUploadAggregator(uploadDir, maxUploadLength)) // 对上传文件做特殊处理，写到一个文件中
            pipeline.addLast("uploader2", new HttpFileUploadAggregator2(uploadDir, maxUploadLength)) // 对上传文件做特殊处理，写到一个文件中, 一次完成解析
            pipeline.addLast("aggregator", new HttpChunkAggregator(maxContentLength)) // 将chunked message合并成一个message
            pipeline.addLast("encoder", new HttpResponseEncoder())
            pipeline.addLast("chunkedWriter", new ChunkedWriteHandler())
            pipeline.addLast("compressor", new HttpContentCompressor())
            pipeline.addLast("handler", nettyHttpServerHandler)
            pipeline;
        }
    }

}


