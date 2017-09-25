package scalabpe.core

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashSet

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
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
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler
import org.jboss.netty.handler.timeout.IdleStateEvent
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.ThreadNameDeterminer
import org.jboss.netty.util.ThreadRenamingRunnable
import org.jboss.netty.util.Timeout
import org.jboss.netty.util.TimerTask;

// used by netty
trait Soc4Netty {
    def connected(connId: String, addr: String, connidx: Int): Unit;
    def disconnected(connId: String, addr: String, connidx: Int): Unit;
    def receive(res: ChannelBuffer, connId: String): Tuple2[Boolean, Int]; // (true,sequence) or (false,0)
    def networkError(sequence: Int, connId: String): Unit;
    def timeoutError(sequence: Int, connId: String): Unit;
    def generatePing(): ChannelBuffer;
    def generateReportSpsId(): ChannelBuffer;
}

object NettyClient {
    val count = new AtomicInteger(1)
    val localAddrsMap = new ConcurrentHashMap[String, String]()
}

class NettyClient(
        val soc: Soc4Netty,
        val addrstr: String,
        val connectTimeout: Int = 15000,
        val pingInterval: Int = 60000,
        val maxPackageSize: Int = 2000000,
        val connSizePerAddr: Int = 8,
        val timerInterval: Int = 100,
        val reconnectInterval: Int = 1,
        var bossExecutor: ThreadPoolExecutor = null,
        var workerExecutor: ThreadPoolExecutor = null,
        var timer: HashedWheelTimer = null,
        var qte: QuickTimerEngine = null,
        val waitForAllConnected: Boolean = false,
        val waitForAllConnectedTimeout: Int = 60000,
        val connectOneByOne: Boolean = false,
        val reuseAddress: Boolean = false,
        val startPort: Int = -1,
        val isSps: Boolean = false) extends Logging with Dumpable {

    var factory: NioClientSocketChannelFactory = _
    var bootstrap: ClientBootstrap = _
    var channelHandler: ChannelHandler = _

    val guids = new Array[String](connSizePerAddr) // guid array

    var addrs = genAddrInfos(addrstr)
    var allConns = new Array[ConnInfo](addrs.size * connSizePerAddr)
    val channelsMap = new HashMap[String, Channel]() // map for special use
    var nextIdx = 0
    val lock = new ReentrantLock(false)

    var portIdx = new AtomicInteger(startPort)
    val dataMap = new ConcurrentHashMap[Int, TimeoutInfo]()

    val connected = new AtomicBoolean()
    val shutdown = new AtomicBoolean()

    var bossThreadFactory: NamedThreadFactory = null
    var workThreadFactory: NamedThreadFactory = null
    var timerThreadFactory: NamedThreadFactory = null

    var useInternalExecutor = true
    var useInternalTimer = true
    var useInternalQte = true
    var qteTimeoutFunctionId = 0

    init

    def uniqueAddrs(s: String): String = {
        if (s == "") return s
        val ss = s.split(",")
        val map = new LinkedHashSet[String]()
        for (k <- ss) map.add(k)
        map.mkString(",")
    }

    def addrsToString(): String = {
        val b = new StringBuilder()
        for (ai <- addrs) {
            if (b.length > 0) b.append(",")
            b.append(ai.addr + ":" + ai.enabled)
        }
        b.toString
    }

    def genAddrInfos(s: String): Array[AddrInfo] = {
        val us = uniqueAddrs(s)
        if (us == "") return new Array[AddrInfo](0)
        val ss = us.split(",")
        val aa = new Array[AddrInfo](ss.length)
        for (i <- 0 until aa.length) aa(i) = new AddrInfo(ss(i))
        aa
    }

    def reconfig(addrstr: String) {
        val us = uniqueAddrs(addrstr)
        var us_array = if (us == "") new Array[String](0) else us.split(",")
        val add = us_array.filter(s => addrs.filter(_.addr == s).size == 0)

        lock.lock()
        try {
            var new_addrs = addrs
            var new_allConns = allConns

            if (add.size > 0) {
                new_addrs = new Array[AddrInfo](addrs.size + add.size)
                for (i <- 0 until addrs.size) new_addrs(i) = addrs(i)
                for (i <- 0 until add.size) new_addrs(addrs.size + i) = new AddrInfo(add(i))
                new_allConns = new Array[ConnInfo](new_addrs.size * connSizePerAddr) // 创建新数组,只扩大不缩小
                for (connInfo <- allConns) {
                    val idx = connInfo.hostidx + connInfo.connidx * new_addrs.size // 复制到新位置
                    new_allConns(idx) = connInfo
                }
            }

            for (idx <- 0 until new_addrs.size) {
                if (us_array.contains(new_addrs(idx).addr)) new_addrs(idx).enabled = true
                else new_addrs(idx).enabled = false
            }

            for (idx <- 0 until new_allConns.size if new_allConns(idx) != null) { // 已存在的地址可能需要修改状态
                val connInfo = new_allConns(idx)

                if (us_array.contains(connInfo.addr)) { // 地址存在
                    if (!connInfo.enabled.get()) { // 之前被设置禁止了需重新打开；否则什么也不做

                        connInfo.enabled.set(true)

                        if (connInfo.connId == null) { // 连接已断开
                            var ss = connInfo.addr.split(":")
                            var host = ss(0)
                            var port = ss(1).toInt
                            var future: ChannelFuture = null
                            if (startPort == -1) {
                                future = bootstrap.connect(new InetSocketAddress(host, port))
                            } else {
                                future = bootstrap.connect(new InetSocketAddress(host, port), new InetSocketAddress(portIdx.getAndIncrement()))
                                if (portIdx.get() >= 65535) portIdx.set(1025)
                            }

                            future.addListener(new ChannelFutureListener() {
                                def operationComplete(future: ChannelFuture) {
                                    onConnectCompleted(future, new_allConns(idx))
                                }
                            })
                        }

                    }
                } else { // 地址不存在
                    connInfo.enabled.set(false) // 设置为false就可以，连接断开重连会检查此标志；但是若连接不断开不会自动断开
                }

            }

            if (add.size > 0) {
                for (idx <- 0 until new_allConns.size if new_allConns(idx) == null) { // 新增的地址需建连接
                    val hostidx = idx % new_addrs.size
                    val connidx = idx / new_addrs.size
                    val addr = new_addrs(hostidx).addr
                    var ss = addr.split(":")
                    var host = ss(0)
                    var port = ss(1).toInt
                    new_allConns(idx) = new ConnInfo(addr, hostidx, connidx, guids(connidx))

                    var future: ChannelFuture = null
                    if (startPort == -1) {
                        future = bootstrap.connect(new InetSocketAddress(host, port))
                    } else {
                        future = bootstrap.connect(new InetSocketAddress(host, port), new InetSocketAddress(portIdx.getAndIncrement()))
                        if (portIdx.get() >= 65535) portIdx.set(1025)
                    }

                    future.addListener(new ChannelFutureListener() {
                        def operationComplete(future: ChannelFuture) {
                            onConnectCompleted(future, new_allConns(idx))
                        }
                    })

                }

                addrs = new_addrs
                allConns = new_allConns
            }

        } finally {
            lock.unlock()
        }
    }

    def dump() {

        log.info("--- addrs=" + addrsToString)

        val buff = new StringBuilder

        buff.append("timer.threads=").append(1).append(",")
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")
        buff.append("channels.size=").append(allConns.size).append(",")

        val connectedCount = allConns.filter(_.ch != null).size

        buff.append("connectedCount=").append(connectedCount).append(",")
        buff.append("dataMap.size=").append(dataMap.size).append(",")
        buff.append("channelsMap.size=").append(channelsMap.size).append(",")

        log.info(buff.toString)

        qte.dump()
    }

    def init(): Unit = {

        channelHandler = new ChannelHandler(this)

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        if (bossExecutor != null || workerExecutor != null) useInternalExecutor = false
        if (timer != null) useInternalTimer = false
        if (qte != null) {
            useInternalQte = false
            qteTimeoutFunctionId = qte.registerAdditionalTimeoutFunction(onTimeout)
            log.info("qteTimeoutFunctionId=" + qteTimeoutFunctionId)
        }
        if (bossExecutor == null) {
            bossThreadFactory = new NamedThreadFactory("socboss" + NettyClient.count.getAndIncrement())
            bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
        }
        if (workerExecutor == null) {
            workThreadFactory = new NamedThreadFactory("socwork" + NettyClient.count.getAndIncrement())
            workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]
        }
        if (timer == null) {
            timerThreadFactory = new NamedThreadFactory("soctimer" + NettyClient.count.getAndIncrement())
            timer = new HashedWheelTimer(timerThreadFactory, 1, TimeUnit.SECONDS)
        }
        if (qte == null) {
            qte = new QuickTimerEngine(onTimeout, timerInterval)
            qteTimeoutFunctionId = 0
        }

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

        for (connidx <- 0 until connSizePerAddr)
            guids(connidx) = java.util.UUID.randomUUID().toString().replaceAll("-", "").toUpperCase

    }

    def start(): Unit = {
        for (hostidx <- 0 until addrs.size) {

            val addr = addrs(hostidx).addr
            var ss = addr.split(":")
            var host = ss(0)
            var port = ss(1).toInt

            for (connidx <- 0 until connSizePerAddr) {

                val idx = hostidx + connidx * addrs.size
                allConns(idx) = new ConnInfo(addr, hostidx, connidx, guids(connidx))

                var future: ChannelFuture = null
                if (startPort == -1) {
                    future = bootstrap.connect(new InetSocketAddress(host, port))
                } else {
                    future = bootstrap.connect(new InetSocketAddress(host, port), new InetSocketAddress(portIdx.getAndIncrement()))
                    if (portIdx.get() >= 65535) portIdx.set(1025)
                }

                future.addListener(new ChannelFutureListener() {
                    def operationComplete(future: ChannelFuture) {
                        onConnectCompleted(future, allConns(idx))
                    }
                })

                if (waitForAllConnected) {

                    lock.lock()
                    try {
                        allConns(idx).future = future
                        allConns(idx).futureStartTime = System.currentTimeMillis
                    } finally {
                        lock.unlock()
                    }

                    // one by one
                    if (connectOneByOne)
                        future.awaitUninterruptibly(connectTimeout, TimeUnit.MILLISECONDS);

                }
            }
        }

        if (waitForAllConnected) {

            val startTs = System.currentTimeMillis
            var t = 0L

            while (!shutdown.get() && !connected.get() && (t - startTs) < waitForAllConnectedTimeout) {

                try {
                    Thread.sleep(1000)
                } catch {
                    case e: Exception =>
                        shutdown.set(true)
                }

                if (!shutdown.get()) {

                    lock.lock()
                    try {

                        t = System.currentTimeMillis
                        for (i <- 0 to allConns.size - 1 if !allConns(i).future.isDone()) {
                            if ((t - allConns(i).futureStartTime) >= (connectTimeout + 2000)) {
                                log.error("connect timeout, cancel manually, idx=" + i) // sometimes connectTimeoutMillis not work!!!
                                allConns(i).future.cancel()
                            }
                        }

                    } finally {
                        lock.unlock()
                    }

                }

            }

            for (i <- 0 to allConns.size - 1) {
                allConns(i).future = null
            }

            val endTs = System.currentTimeMillis()
            log.info("waitForAllConnected finished, connectedCount=" + connectedCount() + ", channels.size=" + allConns.size + ", ts=" + (endTs - startTs) + "ms")

        } else {

            if (addrs.size > 0) {
                val maxWait = connectTimeout.min(2000)
                val now = System.currentTimeMillis
                var t = 0L
                while (!connected.get() && (t - now) < maxWait) {
                    Thread.sleep(50)
                    t = System.currentTimeMillis
                }
            }

        }

        log.info("netty client started, {}, connected={}", addrsToString, connected.get())
    }

    def close(): Unit = {

        shutdown.set(true)

        if (factory != null) {

            log.info("stopping netty client {}", addrsToString)

            if (useInternalTimer)
                timer.stop()
            timer = null

            val allChannels = new DefaultChannelGroup("netty-client-scala")
            for (conn <- allConns if conn.ch != null if conn.ch.isOpen) {
                allChannels.add(conn.ch)
            }
            val future = allChannels.close()
            future.awaitUninterruptibly()

            if (useInternalExecutor)
                factory.releaseExternalResources()
            factory = null
        }

        if (useInternalQte)
            qte.close()

        log.info("netty client stopped {}", addrsToString)
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301001

        lock.lock()
        try {

            var i = 0
            while (i < addrs.size) {
                if (allConns(i).ch == null) {
                    val msg = "sos [" + addrs(i) + "] has error"
                    buff += new SelfCheckResult("SCALABPE.SOS", errorId, true, msg)
                }

                i += 1
            }

        } finally {
            lock.unlock()
        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.SOS", errorId)
        }

        buff
    }

    def reconnect(connInfo: ConnInfo) {
        if (!connInfo.enabled.get()) {
            log.info("reconnect disabled, hostidx={},connidx={}", connInfo.hostidx, connInfo.connidx)
            return
        }
        var ss = connInfo.addr.split(":")
        var host = ss(0)
        var port = ss(1).toInt

        log.info("reconnect called, hostidx={},connidx={}", connInfo.hostidx, connInfo.connidx)

        var future: ChannelFuture = null
        if (startPort == -1) {
            future = bootstrap.connect(new InetSocketAddress(host, port))
        } else {
            future = bootstrap.connect(new InetSocketAddress(host, port), new InetSocketAddress(portIdx.getAndIncrement()))
            if (portIdx.get() >= 65535) portIdx.set(1)
        }

        future.addListener(new ChannelFutureListener() {
            def operationComplete(future: ChannelFuture) {
                onConnectCompleted(future, connInfo)
            }
        })

        if (waitForAllConnected) {

            lock.lock()
            try {
                connInfo.future = future
                connInfo.futureStartTime = System.currentTimeMillis
            } finally {
                lock.unlock()
            }

        }

    }

    def connectedCount(): Int = {

        lock.lock()

        try {

            var i = 0
            var cnt = 0
            while (i < allConns.size) {
                val ch = allConns(i).ch
                if (ch != null) {
                    cnt += 1
                }
                i += 1
            }

            cnt

        } finally {
            lock.unlock()
        }

    }

    def onConnectCompleted(f: ChannelFuture, connInfo: ConnInfo): Unit = {

        if (f.isCancelled()) {

            log.error("connect cancelled, hostidx=%d,connidx=%d".format(connInfo.hostidx, connInfo.connidx))

            if (timer != null) { // while shutdowning
                timer.newTimeout(new TimerTask() {

                    def run(timeout: Timeout) {
                        reconnect(connInfo)
                    }

                }, reconnectInterval, TimeUnit.SECONDS)
            }

        } else if (!f.isSuccess()) {

            log.error("connect failed, hostidx=%d,connidx=%d,e=%s".format(connInfo.hostidx, connInfo.connidx, f.getCause.getMessage))

            if (timer != null) { // while shutdowning
                timer.newTimeout(new TimerTask() {

                    def run(timeout: Timeout) {
                        reconnect(connInfo)
                    }

                }, reconnectInterval, TimeUnit.SECONDS)
            }
        } else {

            var ch = f.getChannel
            var ignore_ch: Channel = null

            lock.lock()

            try {
                if (connInfo.ch == null) {
                    val theConnId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
                    connInfo.ch = ch
                    connInfo.connId = theConnId
                    log.info("connect ok, hostidx=%d,connidx=%d,channelId=%s,channelAddr=%s,clientAddr=%s".format(connInfo.hostidx, connInfo.connidx, ch.getId, connInfo.addr, ch.getLocalAddress.toString))
                } else {
                    log.info("connect ignored, hostidx=%d,connidx=%d,channelId=%s,channelAddr=%s,clientAddr=%s".format(connInfo.hostidx, connInfo.connidx, ch.getId, connInfo.addr, ch.getLocalAddress.toString))
                    ignore_ch = ch
                    ch = null
                }
            } finally {
                lock.unlock()
            }

            if (waitForAllConnected) {

                if (connectedCount() == allConns.size) {
                    connected.set(true)
                }

            } else {
                connected.set(true)
            }

            if (ch == null) {
                if (ignore_ch != null) ignore_ch.close()
            } else {

                soc.connected(connInfo.connId, connInfo.addr, connInfo.connidx)

                val ladr = ch.getLocalAddress.toString
                if (NettyClient.localAddrsMap.contains(ladr)) {
                    log.warn("client addr duplicated, " + ladr)
                } else {
                    NettyClient.localAddrsMap.put(ladr, "1")
                }

                if (isSps) {
                    val buff = soc.generateReportSpsId()
                    if (buff != null) {
                        updateSpsId(buff, connInfo)
                        ch.write(buff);
                    }
                }
            }
        }

    }

    def closeChannelFromOutside(theConnId: String) {

        lock.lock()

        try {
            val channel = channelsMap.getOrElse(theConnId, null)
            if (channel != null && channel.isOpen) {
                channel.close()
                return
            }

        } finally {
            lock.unlock()
        }
    }

    def addChannelToMap(theConnId: String) {

        lock.lock()

        try {
            var i = 0

            while (i < allConns.size) {

                val channel = allConns(i).ch
                val connId = allConns(i).connId
                if (channel != null && channel.isOpen && connId == theConnId) {
                    channelsMap.put(connId, channel)
                    return
                }

                i += 1
            }

        } finally {
            lock.unlock()
        }

    }

    def selectChannel(): String = {

        lock.lock()

        try {
            var i = 0

            while (i < allConns.size) {

                val channel = allConns(i).ch
                val connId = allConns(i).connId
                if (channel != null && channel.isOpen && !channelsMap.contains(connId)) {
                    channelsMap.put(connId, channel)
                    return connId
                }

                i += 1
            }

            null

        } finally {
            lock.unlock()
        }

    }

    def nextChannelFromMap(sequence: Int, timeout: Int, connId: String): Channel = {

        lock.lock()

        try {

            val ch = channelsMap.getOrElse(connId, null)
            if (ch == null) return null

            if (ch.isOpen) {
                val t = qte.newTimer(timeout, sequence, qteTimeoutFunctionId)
                val ti = new TimeoutInfo(sequence, connId, t)
                dataMap.put(sequence, ti)
                return ch
            } else {
                log.error("channel not opened, connId={}", connId)
                removeChannel(connId)
                return null
            }

        } finally {
            lock.unlock()
        }
    }

    def nextChannel(sequence: Int, timeout: Int, addr: String = null): Tuple3[ConnInfo, Channel, Int] = {

        lock.lock()

        try {

            var i = 0
            while (i < allConns.size) {
                val connInfo = allConns(nextIdx)
                val ch = connInfo.ch
                val connId = connInfo.connId
                val chAddr = connInfo.addr
                if (ch != null) { // && ch.isWritable

                    if (ch.isOpen) {

                        var matchAddr = true
                        if (addr != null && chAddr != addr) { // change to domain name match
                            matchAddr = false
                        }

                        if (matchAddr) {
                            val t = qte.newTimer(timeout, sequence, qteTimeoutFunctionId)
                            val ti = new TimeoutInfo(sequence, connId, t)
                            dataMap.put(sequence, ti)

                            val d = (connInfo, ch, nextIdx)
                            nextIdx += 1
                            if (nextIdx >= allConns.size) nextIdx = 0
                            return d
                        }

                    } else {
                        log.error("channel not opened, idx={}, connId={}", i, connId)
                        removeChannel(connId)
                    }

                }
                i += 1
                nextIdx += 1
                if (nextIdx >= allConns.size) nextIdx = 0
            }

            return (null, null, 0)

        } finally {
            lock.unlock()
        }
    }

    def removeChannel(connId: String): Unit = {

        if (shutdown.get()) {
            return
        }

        lock.lock()

        var connInfo: ConnInfo = null
        try {
            var i = 0

            while (connInfo == null && i < allConns.size) {
                val channel = allConns(i).ch
                val theConnId = allConns(i).connId
                if (channel != null && theConnId == connId) {
                    connInfo = allConns(i)
                    soc.disconnected(connInfo.connId, connInfo.addr, connInfo.connidx)
                    connInfo.ch = null
                    connInfo.connId = null
                }

                i += 1
            }

            channelsMap.remove(connId)

        } finally {
            lock.unlock()
        }

        if (connInfo != null) {

            timer.newTimeout(new TimerTask() {

                def run(timeout: Timeout) {
                    reconnect(connInfo)
                }

            }, reconnectInterval, TimeUnit.SECONDS)
        }

    }

    def send(sequence: Int, buff: ChannelBuffer, timeout: Int): Boolean = {

        val (connInfo, ch, idx) = nextChannel(sequence, timeout)

        if (connInfo == null) {
            return false
        }

        if (isSps) updateSpsId(buff, connInfo)
        ch.write(buff);

        return true
    }

    def sendByAddr(sequence: Int, buff: ChannelBuffer, timeout: Int, addr: String): Boolean = {

        val (connInfo, ch, idx) = nextChannel(sequence, timeout, addr)

        if (connInfo == null) {
            return false
        }

        if (isSps) updateSpsId(buff, connInfo)
        ch.write(buff);

        return true
    }

    def updateSpsId(buff: ChannelBuffer, connInfo: ConnInfo) {
        TlvCodec4Xhead.updateSpsId(buff,connInfo.guid)
    }

    def sendByConnId(sequence: Int, buff: ChannelBuffer, timeout: Int, connId: String): Boolean = {

        var ch = nextChannelFromMap(sequence, timeout, connId)

        if (ch == null) {
            return false
        }
        ch.write(buff);

        return true
    }

    def sendResponse(sequence: Int, buff: ChannelBuffer, connId: String): Boolean = {

        var ch: Channel = null

        lock.lock()

        try {
            var i = 0

            while (i < allConns.size && ch == null) {
                val channel = allConns(i).ch
                val theConnId = allConns(i).connId
                if (channel != null && theConnId == connId) {
                    ch = allConns(i).ch
                }

                i += 1
            }
        } finally {
            lock.unlock()
        }

        if (ch == null) {
            return false
        }

        ch.write(buff);

        return true
    }

    def parseIpPort(s: String): String = {

        val p = s.indexOf("/")

        if (p >= 0)
            s.substring(p + 1)
        else
            s
    }

    def onTimeout(data: Any): Unit = {

        val sequence = data.asInstanceOf[Int]

        val ti = dataMap.remove(sequence)
        if (ti != null) {
            soc.timeoutError(sequence, ti.connId)
        } else {
            log.error("timeout but sequence not found, seq={}", sequence)
        }

    }

    def onReceive(buff: ChannelBuffer, connId: String): Unit = {

        val (ok, sequence) = soc.receive(buff, connId)

        if (ok) {

            //log.info("onReceive,seq="+sequence)

            val ti = dataMap.remove(sequence)
            if (ti != null) {
                ti.timer.cancel()
            } else {
                log.warn("receive but sequence not found, seq={}", sequence)
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
                log.error("network error but sequence not found, seq={}", sequence)
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
        val buff = soc.generatePing()
        ch.write(buff);
    }

    class PipelineFactory extends Object with ChannelPipelineFactory {

        def getPipeline(): ChannelPipeline = {
            val pipeline = Channels.pipeline();
            val decoder = new LengthFieldBasedFrameDecoder(maxPackageSize, 4, 4, -8, 0);
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, pingInterval / 1000));
            pipeline.addLast("decoder", decoder);
            pipeline.addLast("handler", channelHandler);
            pipeline;
        }
    }

    class ChannelHandler(val nettyClient: NettyClient) extends IdleStateAwareChannelHandler with Logging {

        override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
            nettyClient.messageReceived(ctx, e)
        }

        override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent): Unit = {
            nettyClient.channelIdle(ctx, e)
        }

        override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
            nettyClient.exceptionCaught(ctx, e)
        }

        override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            nettyClient.channelConnected(ctx, e)
        }

        override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            nettyClient.channelDisconnected(ctx, e)
        }

    }

    class TimeoutInfo(val sequence: Int, val connId: String, val timer: QuickTimer)

    class AddrInfo(val addr: String, var enabled: Boolean = true)

    class ConnInfo(val addr: String, val hostidx: Int, val connidx: Int, val guid: String) {
        val enabled = new AtomicBoolean(true)
        var ch: Channel = null
        var connId: String = null
        var future: ChannelFuture = null
        var futureStartTime: Long = 0
    }

}


