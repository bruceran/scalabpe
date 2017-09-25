package scalabpe.plugin.cache

import java.net.InetSocketAddress
import java.util.LinkedList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler
import org.jboss.netty.handler.timeout.IdleStateEvent
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.ThreadNameDeterminer
import org.jboss.netty.util.ThreadRenamingRunnable
import org.jboss.netty.util.Timeout
import org.jboss.netty.util.TimerTask

import scalabpe.core.ArrayBufferInt
import scalabpe.core.Dumpable
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.QuickTimer
import scalabpe.core.QuickTimerEngine
import scalabpe.core.SelfCheckResult

object RedisNettyClient4Cluster {
    val count = new AtomicInteger(1)
}

class RedisNettyClient4Cluster(
        val soc: RedisSoc4Cluster,
        val addrstr: String,
        val connectTimeout: Int = 15000,
        val pingInterval: Int = 60000,
        val timerInterval: Int = 100,
        val reconnectInterval: Int = 1,
        val reuseAddress: Boolean = false) extends Logging with Dumpable {

    var bossThreadFactory: NamedThreadFactory = _
    var workThreadFactory: NamedThreadFactory = _
    var timerThreadFactory: NamedThreadFactory = _
    var factory: NioClientSocketChannelFactory = _
    var bootstrap: ClientBootstrap = _
    var channelHandler: ChannelHandler = _
    var bossExecutor: ThreadPoolExecutor = _
    var workerExecutor: ThreadPoolExecutor = _
    var timer: HashedWheelTimer = _
    var qte: QuickTimerEngine = _

    val slots = new Array[SlotInfo](16384) // 每个slot对应的地址
    val slotsLock = new ReentrantLock(false)

    val conns = new ConcurrentHashMap[String, ConnInfo] // 每个地址对应的连接

    val dataMap = new ConcurrentHashMap[Int, TimeoutInfo]()

    val connected = new AtomicBoolean()
    val shutdown = new AtomicBoolean()

    init

    def dump() {

        log.info("--- addrstr=" + addrstr)

        val buff = new StringBuilder

        buff.append("timer.threads=").append(1).append(",")
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")
        buff.append("channels.size=").append(conns.size).append(",")

        buff.append("connectedCount=").append(connectedCount()).append(",")
        buff.append("dataMap.size=").append(dataMap.size).append(",")

        log.info(buff.toString)

        qte.dump()
    }

    def init(): Unit = {

        channelHandler = new ChannelHandler(this)

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        bossThreadFactory = new NamedThreadFactory("redisboss" + RedisNettyClient4Cluster.count.getAndIncrement())
        bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
        workThreadFactory = new NamedThreadFactory("rediswork" + RedisNettyClient4Cluster.count.getAndIncrement())
        workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]
        timerThreadFactory = new NamedThreadFactory("redistimer" + RedisNettyClient4Cluster.count.getAndIncrement())
        timer = new HashedWheelTimer(timerThreadFactory, 1, TimeUnit.SECONDS)
        qte = new QuickTimerEngine(onTimeout, timerInterval)

        factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor)
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new PipelineFactory());

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("connectTimeoutMillis", connectTimeout);

        if (reuseAddress)
            bootstrap.setOption("reuseAddress", true);
        else
            bootstrap.setOption("reuseAddress", false);

        val addrs = addrstr.split(",")
        for (i <- 0 until addrs.size) {

            val connInfo = new ConnInfo(addrs(i))
            conns.put(connInfo.addr, connInfo)

            var ss = connInfo.addr.split(":")
            var host = ss(0)
            var port = ss(1).toInt

            val future = bootstrap.connect(new InetSocketAddress(host, port))

            future.addListener(new ChannelFutureListener() {
                def operationComplete(future: ChannelFuture) {
                    onConnectCompleted(future, connInfo)
                }
            })

        }

        val maxWait = connectTimeout.min(5000)
        val now = System.currentTimeMillis
        var t = 0L
        while (!connected.get() && (t - now) < maxWait) {
            Thread.sleep(50)
            t = System.currentTimeMillis
        }

        val first = getFirstConn()
        if (first != null) {
            slots(0) = new SlotInfo(first) // 设一个默认地址，避免无法提供服务
        }

        log.info("nettyredisclient4cluster started, {}, connected={}", addrstr, connected.get())
    }

    def getFirstConn(): String = {
        val addrs = addrstr.split(",")
        for (i <- 0 until addrs.size) {
            val connInfo = conns.get(addrs(i))
            if (connInfo != null) {
                connInfo.lock.lock()
                try {
                    if (connInfo.ch != null) {
                        return connInfo.addr
                    }
                } finally {
                    connInfo.lock.unlock()
                }
            }
        }
        null
    }

    def close(): Unit = {

        shutdown.set(true)

        if (factory != null) {

            log.info("stopping nettyredisclient4cluster {}", addrstr)

            timer.stop()
            timer = null

            val allChannels = new DefaultChannelGroup("netty-client-redis-scala")
            val it = conns.values().iterator()
            while (it.hasNext()) {
                val connInfo = it.next()
                connInfo.lock.lock()
                try {
                    if (connInfo.ch != null && connInfo.ch.isOpen) {
                        allChannels.add(connInfo.ch)
                    }
                } finally {
                    connInfo.lock.unlock()
                }
            }
            val future = allChannels.close()
            future.awaitUninterruptibly()

            factory.releaseExternalResources()
            factory = null
        }

        qte.close()

        log.info("nettyredisclient4cluster stopped {}", addrstr)
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301001

        val it = conns.values().iterator()
        while (it.hasNext()) {
            val connInfo = it.next()
            connInfo.lock.lock()
            try {
                if (connInfo.ch == null) {
                    val msg = "sos [" + connInfo.addr + "] has error"
                    buff += new SelfCheckResult("SCALABPE.REDIS", errorId, true, msg)
                }
            } finally {
                connInfo.lock.unlock()
            }
        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.REDIS", errorId)
        }

        buff
    }

    def reconnect(connInfo: ConnInfo) {

        // 超过5分钟并且不在slot的地址里的，删除连接不再重试
        connInfo.lock.lock()
        try {
            val now = System.currentTimeMillis
            if (now - connInfo.disconnected >= 5 * 60 * 1000) {
                if (!checkAddrValid(connInfo.addr)) {
                    log.warn("addr is not valid after 5 minutes, remove it, addr={}", connInfo.addr)
                    conns.remove(connInfo.addr)
                    return
                }
            }
        } finally {
            connInfo.lock.unlock()
        }

        var ss = connInfo.addr.split(":")
        var host = ss(0)
        var port = ss(1).toInt

        log.info("reconnect called, addr={}", connInfo.addr)

        val future = bootstrap.connect(new InetSocketAddress(host, port))

        future.addListener(new ChannelFutureListener() {
            def operationComplete(future: ChannelFuture) {
                onConnectCompleted(future, connInfo)
            }
        })

    }

    def onConnectCompleted(f: ChannelFuture, connInfo: ConnInfo): Unit = {

        if (f.isCancelled()) {

            log.error("connect cancelled, addr=%s".format(connInfo.addr))

            if (timer != null) { // while shutdowning
                timer.newTimeout(new TimerTask() {

                    def run(timeout: Timeout) {
                        reconnect(connInfo)
                    }

                }, reconnectInterval, TimeUnit.SECONDS)
            }

        } else if (!f.isSuccess()) {

            log.error("connect failed, addr=%s,e=%s".format(connInfo.addr, f.getCause.getMessage))

            if (timer != null) { // while shutdowning
                timer.newTimeout(new TimerTask() {

                    def run(timeout: Timeout) {
                        reconnect(connInfo)
                    }

                }, reconnectInterval, TimeUnit.SECONDS)
            }
        } else {

            val ch = f.getChannel
            log.info("connect ok, addr=%s,channelId=%s,clientAddr=%s".format(connInfo.addr, ch.getId, ch.getLocalAddress.toString))

            connInfo.lock.lock()
            try {
                if (connInfo.ch == null) {
                    val theConnId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
                    connInfo.ch = ch
                    connInfo.connId = theConnId
                    connInfo.seqs.clear()
                    connInfo.disconnected = 0
                }
            } finally {
                connInfo.lock.unlock()
            }

            if (connectedCount() >= addrstr.split(",").size) {
                connected.set(true)
            }
        }

    }

    def connectedCount(): Int = {

        var cnt = 0

        val it = conns.values().iterator()
        while (it.hasNext()) {
            val connInfo = it.next()
            connInfo.lock.lock()
            try {
                if (connInfo.ch != null) {
                    cnt += 1
                }
            } finally {
                connInfo.lock.unlock()
            }
        }

        cnt

    }

    def checkAddrValid(addr: String): Boolean = {
        slotsLock.lock()
        try {
            var i = 0
            while (i < slots.length) {
                val v = slots(i)
                if (v.master == addr) return true
                i += 1
            }
            return false
        } finally {
            slotsLock.unlock()
        }
    }

    def getAddr(slot: Int): String = {
        if (slot < 0 || slot >= slots.length) return null
        slotsLock.lock()
        try {
            val v = slots(slot)
            if (v != null) return v.master
            var i = 0
            while (i < slots.length) { // 找第一个有地址的slot用来发送请求
                val v2 = slots(i)
                if (v2 != null) return v2.master
                i += 1
            }
            return null
        } finally {
            slotsLock.unlock()
        }
    }

    def changeSlotAddr(min: Int, max: Int, addr: String) {
        for (i <- min to max) {
            changeSlotAddr(i, addr, wait = false)
        }
    }

    def changeSlotAddr(slot: Int, addr: String, wait: Boolean = true) {
        if (slot < 0 || slot >= slots.length) return

        slotsLock.lock()
        try {
            val v = slots(slot)
            if (v != null && v.master == addr) return
            slots(slot) = new SlotInfo(addr)
            val connInfo = conns.get(addr)
            if (connInfo != null) return
        } finally {
            slotsLock.unlock()
        }

        val connInfo = new ConnInfo(addr)
        conns.put(addr, connInfo)

        val ss = addr.split(":")
        val host = ss(0)
        val port = ss(1).toInt

        val future = bootstrap.connect(new InetSocketAddress(host, port))

        val finished = new AtomicBoolean(false)
        future.addListener(new ChannelFutureListener() {
            def operationComplete(future: ChannelFuture) {
                onConnectCompleted(future, connInfo)
                finished.set(true)
            }
        })

        // 最多等2秒, 不能用future.await, future await返回了不表示onConnectCompleted执行完了
        val now = System.currentTimeMillis
        var t = 0L
        while (!finished.get() && (t - now) < 2000) {
            Thread.sleep(50)
            t = System.currentTimeMillis
        }
    }

    def sendByAddr(sequence: Int, buff: ChannelBuffer, timeout: Int, addr: String, asking: Boolean = false, hasReply: Boolean = true): Boolean = {
        val connInfo = conns.get(addr)
        if (connInfo == null) return false
        connInfo.lock.lock()
        try {
            if (connInfo.ch == null) return false

            if (!connInfo.ch.isOpen) {
                connInfo.ch = null
                connInfo.connId = null
                connInfo.seqs.clear()
                connInfo.disconnected = System.currentTimeMillis
                log.error("channel not opened, connId={}", connInfo.connId)
                return false
            }

            if (asking) {
                val (asking_sequence, asking_buff) = soc.generateAsking()
                val t = qte.newTimer(timeout, asking_sequence)
                val ti = new TimeoutInfo(asking_sequence, connInfo.connId, t)
                dataMap.put(asking_sequence, ti)

                if (hasReply)
                    connInfo.seqs.offer(asking_sequence)

                connInfo.ch.write(asking_buff);
            }

            val t = qte.newTimer(timeout, sequence)
            val ti = new TimeoutInfo(sequence, connInfo.connId, t)
            dataMap.put(sequence, ti)

            if (hasReply)
                connInfo.seqs.offer(sequence)

            connInfo.ch.write(buff);

            return true

        } finally {
            connInfo.lock.unlock()
        }

    }

    def sendBySlot(sequence: Int, buff: ChannelBuffer, timeout: Int, slot: Int, hasReply: Boolean = true): Boolean = {
        val addr = getAddr(slot)
        if (addr == null) return false
        sendByAddr(sequence, buff, timeout, addr, asking = false, hasReply = hasReply)
    }

    def removeChannel(connId: String): Unit = {

        if (shutdown.get()) {
            return
        }

        val addr = parseAddrFromConnId(connId)
        val connInfo = conns.get(addr)
        if (connInfo == null) return

        connInfo.lock.lock()
        try {
            connInfo.ch = null
            connInfo.connId = null
            connInfo.seqs.clear()
            connInfo.disconnected = System.currentTimeMillis
        } finally {
            connInfo.lock.unlock()
        }

        timer.newTimeout(new TimerTask() {

            def run(timeout: Timeout) {
                reconnect(connInfo)
            }

        }, reconnectInterval, TimeUnit.SECONDS)

    }

    def onTimeout(data: Any): Unit = {

        val sequence = data.asInstanceOf[Int]

        val ti = dataMap.remove(sequence)
        if (ti != null) {
            soc.timeoutError(sequence, ti.connId)
        } else {
            //log.error("timeout but sequence not found, seq={}",sequence)
        }

    }

    def getSequenceFromQueue(connId: String): Tuple2[Boolean, Int] = {
        val addr = parseAddrFromConnId(connId)
        val connInfo = conns.get(addr)
        if (connInfo == null) return (false, 0)

        connInfo.lock.lock()
        try {
            val seq = connInfo.seqs.poll()
            return new Tuple2(true, seq)
        } catch {
            case e: Throwable =>
                return new Tuple2(false, 0)
        } finally {
            connInfo.lock.unlock()
        }
    }

    def onReceive(buff: ChannelBuffer, connId: String): Unit = {

        val (ok, sequence) = getSequenceFromQueue(connId)

        if (ok) {

            val ti = dataMap.remove(sequence)
            if (ti != null) {
                ti.timer.cancel()
            } else {
                //log.warn("receive but sequence not found, seq={}",sequence)
            }

            try {
                soc.receive(sequence, buff, connId)
            } catch {
                case e: Throwable =>
                    log.error("exception in receive, e=" + e.getMessage, e)
                    throw e
            }
        }

    }

    def onNetworkError(connId: String): Unit = {

        removeChannel(connId)

        val seqs = new ArrayBufferInt()
        val i = dataMap.values().iterator
        while (i.hasNext()) {
            val info = i.next()
            if (info.connId == connId) {
                seqs += info.sequence
            }
        }

        for (sequence <- seqs) {
            val ti = dataMap.remove(sequence)
            if (ti != null) {
                ti.timer.cancel()
                soc.networkError(sequence, connId)
            } else {
                //log.error("network error but sequence not found, seq={}",sequence)
            }
        }

    }

    def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String]
        val buf = e.getMessage().asInstanceOf[ChannelBuffer]
        onReceive(buf, connId)
    }

    def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
        val ch = e.getChannel
        val connId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
        ctx.setAttachment(connId);
    }

    def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String]
        onNetworkError(connId)
        log.info("channelDisconnected id={}", connId)
    }

    def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String];
        log.error("exceptionCaught connId={},e={}", connId, e)
        if (ch.isOpen)
            ch.close()
    }

    def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent): Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String];
        val addr = parseAddrFromConnId(connId)
        val (sequence, buff) = soc.generatePing()

        val connInfo = conns.get(addr)
        if (connInfo == null) return

        connInfo.lock.lock()
        try {
            if (connInfo.ch == null) return
            connInfo.seqs.offer(sequence)
            connInfo.ch.write(buff);
        } finally {
            connInfo.lock.unlock()
        }
    }

    def parseIpPort(s: String): String = {

        val p = s.indexOf("/")

        if (p >= 0)
            s.substring(p + 1)
        else
            s
    }

    def parseAddrFromConnId(s: String): String = {
        val p = s.lastIndexOf(":")

        if (p >= 0)
            s.substring(0, p)
        else
            s
    }

    class PipelineFactory extends Object with ChannelPipelineFactory {

        def getPipeline(): ChannelPipeline = {
            val pipeline = Channels.pipeline();
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, pingInterval / 1000));
            pipeline.addLast("decoder", new RedisFrameDecoder());
            pipeline.addLast("handler", channelHandler);
            pipeline;
        }
    }

    class ChannelHandler(val client: RedisNettyClient4Cluster) extends IdleStateAwareChannelHandler with Logging {

        override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
            client.messageReceived(ctx, e)
        }

        override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent): Unit = {
            client.channelIdle(ctx, e)
        }

        override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
            client.exceptionCaught(ctx, e)
        }

        override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            client.channelConnected(ctx, e)
        }

        override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            client.channelDisconnected(ctx, e)
        }

    }

    class TimeoutInfo(val sequence: Int, val connId: String, val timer: QuickTimer)

    class SlotInfo(val master: String, var slave: String = null)

    class ConnInfo(val addr: String) {
        val lock = new ReentrantLock(false)
        val seqs = new LinkedList[Int]()
        var ch: Channel = null
        var connId: String = null
        var disconnected = System.currentTimeMillis()
    }

}

