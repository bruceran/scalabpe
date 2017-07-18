package scalabpe.core

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFactory
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler
import org.jboss.netty.handler.timeout.IdleStateEvent
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.ThreadNameDeterminer
import org.jboss.netty.util.ThreadRenamingRunnable
import org.jboss.netty.util.Timer;

// used by netty
trait Sos4Netty {
    def receive(bb: ChannelBuffer, connId: String): Unit;
    def connected(connId: String): Unit;
    def disconnected(connId: String): Unit;
}

class NettyServerHandler(val nettyServer: NettyServer, val sos: Sos4Netty)
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

    def write(connId: String, response: ChannelBuffer): Boolean = {

        if (response == null) return false

        val ch = conns.get(connId)
        if (ch == null) {
            log.error("connection not found, id={}", connId)
            return false;
        }

        if (ch.isOpen) {
            ch.write(response);
            return true
        }

        return false
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {

        val ch = e.getChannel
        if (stopFlag.get()) {
            ch.setReadable(false)
            return
        }

        val buf = e.getMessage().asInstanceOf[ChannelBuffer];
        val connId = ctx.getAttachment().asInstanceOf[String];

        try {
            sos.receive(buf, connId);
        } catch {
            case e: Exception =>
                log.error("sos decode error, connId=" + connId, e);
        }
    }

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {

        new_conns.incrementAndGet()

        val ch = e.getChannel
        val connId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
        ctx.setAttachment(connId);

        if (conns.size >= nettyServer.maxConns) {
            log.error("connection started, id={}, but max connections exceeded, conn not allowed", connId)
            ch.close
            return
        }

        log.info("connection started, id={}", connId)

        conns.put(connId, ch)
        sos.connected(connId)

        if (stopFlag.get()) {
            ch.close
        }
    }

    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {

        new_disconns.incrementAndGet()

        val connId = ctx.getAttachment().asInstanceOf[String];
        log.info("connection ended, id={}", connId)
        conns.remove(connId)
        sos.disconnected(connId)
    }

    override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent): Unit = {
        val connId = ctx.getAttachment().asInstanceOf[String];
        log.error("connection timeout, id={}", connId)
        e.getChannel.close()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
        val connId = ctx.getAttachment().asInstanceOf[String];
        log.error("connection exception, id=%s,msg=%s".format(connId, e.toString),e.getCause)
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

class NettyServer(val sos: Sos4Netty,
                  val port: Int,
                  val host: String = "*",
                  val idleTimeoutMillis: Int = 180000,
                  val maxPackageSize: Int = 2000000,
                  val maxConns: Int = 500000) extends Logging with Dumpable {

    var nettyServerHandler: NettyServerHandler = _
    var allChannels: ChannelGroup = _
    var channelFactory: ChannelFactory = _
    var timer: Timer = _

    val bossThreadFactory = new NamedThreadFactory("sosboss")
    val workThreadFactory = new NamedThreadFactory("soswork")
    val timerThreadFactory = new NamedThreadFactory("sostimer")

    var bossExecutor: ThreadPoolExecutor = _
    var workerExecutor: ThreadPoolExecutor = _

    def stats(): Array[Int] = {
        nettyServerHandler.stats
    }

    def dump() {
        if (nettyServerHandler == null) return

        val buff = new StringBuilder

        buff.append("nettyServerHandler.conns.size=").append(nettyServerHandler.conns.size).append(",")
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")

        log.info(buff.toString)
    }

    def start(): Unit = {

        nettyServerHandler = new NettyServerHandler(this, sos)

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        timer = new HashedWheelTimer(timerThreadFactory, 1, TimeUnit.SECONDS)

        allChannels = new DefaultChannelGroup("netty-server-scala")

        bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
        workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]
        channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor)

        val bootstrap = new ServerBootstrap(channelFactory)
        bootstrap.setPipelineFactory(new PipelineFactory())
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

        val s = "netty tcp server started on host(" + host + ") port(" + port + ")"
        log.info(s)
    }

    def write(connId: String, response: ChannelBuffer): Boolean = {
        nettyServerHandler.write(connId, response)
    }

    def closeReadChannel() {
        if (nettyServerHandler != null) {
            nettyServerHandler.close()
        }
        log.info("nettyServerHandler read channel stopped")
    }

    def close(): Unit = {

        if (channelFactory != null) {

            log.info("Stopping NettyServer")

            timer.stop()
            timer = null

            val chs = nettyServerHandler.conns.values.iterator
            while (chs.hasNext()) {
                allChannels.add(chs.next())
            }
            val future = allChannels.close()
            future.awaitUninterruptibly()
            allChannels = null

            channelFactory.releaseExternalResources()
            channelFactory = null

            log.info("netty server stopped")
        }
    }

    class PipelineFactory extends Object with ChannelPipelineFactory {

        def getPipeline(): ChannelPipeline = {
            val pipeline = Channels.pipeline();
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, idleTimeoutMillis / 1000));
            val decoder = new LengthFieldBasedFrameDecoder(maxPackageSize, 4, 4, -8, 0);
            pipeline.addLast("decoder", decoder);
            pipeline.addLast("handler", nettyServerHandler);
            pipeline;
        }
    }

}


