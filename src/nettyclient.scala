package jvmdbbroker.core

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock;
import java.nio.ByteBuffer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import scala.collection.mutable.{ArrayBuffer,HashMap}

import org.jboss.netty.buffer._;
import org.jboss.netty.channel._;
import org.jboss.netty.handler.timeout._;
import org.jboss.netty.bootstrap._;
import org.jboss.netty.channel.group._;
import org.jboss.netty.channel.socket.nio._;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util._;

// used by netty
trait Soc4Netty {
    def receive(res:ByteBuffer,connId:String):Tuple2[Boolean,Int]; // (true,sequence) or (false,0)
    def networkError(sequence:Int,connId:String):Unit;
    def timeoutError(sequence:Int,connId:String):Unit;
    def generatePing():ByteBuffer;
    def generateReportSpsId():ByteBuffer;
}

object NettyClient {
    val count = new AtomicInteger(1)
    val localAddrsMap = new ConcurrentHashMap[String,String]()
}

class NettyClient(
    val soc: Soc4Netty,
    val addrstr: String,
    val connectTimeout :Int = 15000,
    val pingInterval: Int = 60000,
    val maxPackageSize: Int = 2000000,
    val connSizePerAddr: Int = 8 ,
    val timerInterval :Int = 100,
    val reconnectInterval : Int = 1,
    var bossExecutor:ThreadPoolExecutor = null,
    var workerExecutor:ThreadPoolExecutor = null,
    var timer : HashedWheelTimer = null,
    var qte : QuickTimerEngine = null,
    val waitForAllConnected : Boolean = false,
    val waitForAllConnectedTimeout :Int = 60000,
    val connectOneByOne :Boolean = false,
    val reuseAddress: Boolean = false,
    val startPort: Int = -1,
    val isSps: Boolean = false ) extends Logging with Dumpable {

    var factory : NioClientSocketChannelFactory = _
    var bootstrap : ClientBootstrap = _
    var channelHandler : ChannelHandler = _

    val addrs = addrstr.split(",")
    val dataMap = new ConcurrentHashMap[Int,TimeoutInfo]()

    val channelAddrs = new Array[String](addrs.size*connSizePerAddr) // domain name array
    val channels = new Array[Channel](addrs.size*connSizePerAddr) // channel array
    val channelIds = new Array[String](addrs.size*connSizePerAddr) // connId array
    val channelsMap = new HashMap[String,Channel]() // map for special use
    val lock = new ReentrantLock(false)

    val guids = new Array[String](connSizePerAddr) // guid array

    var nextIdx = 0
    val connected = new AtomicBoolean()

    val shutdown = new AtomicBoolean()

    var bossThreadFactory : NamedThreadFactory = null
    var workThreadFactory : NamedThreadFactory = null
    var timerThreadFactory : NamedThreadFactory = null

    var useInternalExecutor = true
    var useInternalTimer = true
    var useInternalQte = true
    var qteTimeoutFunctionId = 0

    var portIdx = new AtomicInteger(startPort)

    val futures = new Array[ChannelFuture](addrs.size*connSizePerAddr)
    val futuresStartTime = new Array[Long](addrs.size*connSizePerAddr)
    val futureLock = new ReentrantLock(false)

    init

    def dump() {

        log.info("--- addrstr="+addrstr)

        val buff = new StringBuilder

        buff.append("timer.threads=").append(1).append(",")
        buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
        buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
        buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
        buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")
        buff.append("channels.size=").append(channels.size).append(",")

        val connectedCount = channels.filter( _ != null).size

        buff.append("connectedCount=").append(connectedCount).append(",")
        buff.append("dataMap.size=").append(dataMap.size).append(",")

        log.info(buff.toString)

        qte.dump()
    }

    def init() : Unit  = {

        channelHandler = new ChannelHandler(this)

        // without this line, the thread name of netty will not be changed
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

        if( bossExecutor != null || workerExecutor != null ) useInternalExecutor = false
        if( timer != null ) useInternalTimer = false
        if( qte != null ) {
            useInternalQte = false
            qteTimeoutFunctionId = qte.registerAdditionalTimeoutFunction(onTimeout)
            log.info("qteTimeoutFunctionId="+qteTimeoutFunctionId)
        }
        if( bossExecutor == null ) {
            bossThreadFactory = new NamedThreadFactory("socboss"+NettyClient.count.getAndIncrement())
            bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
        }
        if( workerExecutor == null ) {
            workThreadFactory = new NamedThreadFactory("socwork"+NettyClient.count.getAndIncrement())
            workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]
        }
        if( timer == null ) {
            timerThreadFactory = new NamedThreadFactory("soctimer"+NettyClient.count.getAndIncrement())
            timer = new HashedWheelTimer(timerThreadFactory,1,TimeUnit.SECONDS)
        }
        if( qte == null ) {
            qte = new QuickTimerEngine(onTimeout,timerInterval)
            qteTimeoutFunctionId = 0
        }

        factory = new NioClientSocketChannelFactory(bossExecutor ,workerExecutor)
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new PipelineFactory());

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("connectTimeoutMillis", connectTimeout);

        if( reuseAddress )
            bootstrap.setOption("reuserAddress", true);
        else
            bootstrap.setOption("reuserAddress", false);


        for( connidx <- 0 until connSizePerAddr ) 
            guids(connidx) = java.util.UUID.randomUUID().toString().replaceAll("-", "").toUpperCase

        for(hostidx <- 0 until addrs.size ) {

            var ss = addrs(hostidx).split(":")
            var host = ss(0)
            var port = ss(1).toInt

            for( connidx <- 0 until connSizePerAddr ) {

                val idx = hostidx + connidx * addrs.size
                channelAddrs(idx) = addrs(hostidx)

                var future : ChannelFuture = null
                if( startPort == -1 ) {
                    future = bootstrap.connect(new InetSocketAddress(host,port))
                } else {
                    future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()))
                    if( portIdx.get() >= 65535 ) portIdx.set(1025)
                }

                future.addListener( new ChannelFutureListener() {
                    def operationComplete(future: ChannelFuture ) {
                        onConnectCompleted(future,hostidx,connidx)
                    }
                } )

                if( waitForAllConnected ) {

                    futureLock.lock()
                    try {
                        //val idx = hostidx + connidx * addrs.size
                        futures(idx) = future
                        futuresStartTime(idx) = System.currentTimeMillis
                        }	finally {
                            futureLock.unlock()
                        }

                        // one by one
                        if( connectOneByOne )
                            future.awaitUninterruptibly(connectTimeout, TimeUnit.MILLISECONDS);

                }
            }
        }

        if( waitForAllConnected ) {

            val startTs = System.currentTimeMillis
            var t = 0L

            while( !shutdown.get() && !connected.get() && ( t - startTs ) < waitForAllConnectedTimeout ){

                try {
                    Thread.sleep(1000)
                } catch {
                    case e:Exception =>
                        shutdown.set(true)
                }

                if( !shutdown.get() ) {

                    futureLock.lock()
                    try {

                        t = System.currentTimeMillis
                        for(i <- 0 to futures.size - 1 if !futures(i).isDone() ) {
                            if( ( t - futuresStartTime(i)	) >= (connectTimeout + 2000) ) {
                                log.error("connect timeout, cancel manually, idx="+i) // sometimes connectTimeoutMillis not work!!!
                                futures(i).cancel()
                            }
                        }

                        }	finally {
                            futureLock.unlock()
                        }

                }

            }

            for(i <- 0 to futures.size - 1 ) {
                futures(i) = null
            }

            val endTs = System.currentTimeMillis()
            log.info("waitForAllConnected finished, connectedCount="+connectedCount()+", channels.size="+channels.size+", ts="+(endTs-startTs)+"ms")

        } else {

            val maxWait = connectTimeout.min(2000)
            val now = System.currentTimeMillis
            var t = 0L
            while(!connected.get() && (t - now ) < maxWait){
                Thread.sleep(50)
                t = System.currentTimeMillis
            }

        }

        log.info("netty client started, {}, connected={}",addrstr,connected.get())
    }

    def close() : Unit = {

        shutdown.set(true)

        if (factory != null) {

            log.info("stopping netty client {}",addrstr)

            if( useInternalTimer )
                timer.stop()
            timer = null

            val allChannels = new DefaultChannelGroup("netty-client-scala")
            for(ch <- channels if ch != null if ch.isOpen) {
                allChannels.add(ch)
            }
            val future = allChannels.close()
            future.awaitUninterruptibly()

            if( useInternalExecutor )
                factory.releaseExternalResources()
            factory = null
        }

        if( useInternalQte )
            qte.close()

        log.info("netty client stopped {}",addrstr)
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301001

        var i = 0
        while( i < addrs.size ) {
            if( channels(i) == null ) {
                val msg = "sos ["+addrs(i)+"] has error"
                buff += new SelfCheckResult("JVMDBBRK.SOS",errorId,true,msg)
            }

            i += 1
        }

        if( buff.size == 0 ) {
            buff += new SelfCheckResult("JVMDBBRK.SOS",errorId)
        }

        buff
    }

    def reconnect(hostidx:Int,connidx:Int) {

        var ss = addrs(hostidx).split(":")
        var host = ss(0)
        var port = ss(1).toInt

        log.info("reconnect called, hostidx={},connidx={}",hostidx,connidx)

        var future : ChannelFuture = null
        if( startPort == -1 ) {
            future = bootstrap.connect(new InetSocketAddress(host,port))
        } else {
            future = bootstrap.connect(new InetSocketAddress(host,port),new InetSocketAddress(portIdx.getAndIncrement()))
            if( portIdx.get() >= 65535 ) portIdx.set(1)
        }

        future.addListener( new ChannelFutureListener() {
            def operationComplete(future: ChannelFuture ) {
                onConnectCompleted(future,hostidx,connidx)
            }
        } )

        if( waitForAllConnected ) {

            futureLock.lock()
            try {
                val idx = hostidx + connidx * addrs.size
                futures(idx) = future
                futuresStartTime(idx) = System.currentTimeMillis
                }	finally {
                    futureLock.unlock()
                }

        }

    }

    def connectedCount():Int = {

        lock.lock()

        try {

            var i = 0
            var cnt = 0
            while(  i < channels.size ) {
                val ch = channels(i)
                if( ch != null ) {
                    cnt +=1
                }
                i+=1
            }

            cnt

        } finally {
            lock.unlock()
        }

    }

    def onConnectCompleted(f: ChannelFuture, hostidx:Int, connidx:Int) : Unit = {

        if (f.isCancelled()) {

            log.error("connect cancelled, hostidx=%d,connidx=%d".format(hostidx,connidx))

            if( timer != null ) { // while shutdowning
                timer.newTimeout( new TimerTask() {

                    def run( timeout: Timeout) {
                        reconnect(hostidx,connidx)
                    }

                }, reconnectInterval, TimeUnit.SECONDS)
            }

        } else if (!f.isSuccess()) {

            log.error("connect failed, hostidx=%d,connidx=%d,e=%s".format(hostidx,connidx,f.getCause.getMessage))

            if( timer != null ) { // while shutdowning
                timer.newTimeout( new TimerTask() {

                    def run( timeout: Timeout) {
                        reconnect(hostidx,connidx)
                    }

                }, reconnectInterval, TimeUnit.SECONDS)
            }
        } else {

            val ch = f.getChannel
            log.info("connect ok, hostidx=%d,connidx=%d,channelId=%s,channelAddr=%s,clientAddr=%s".format(hostidx,connidx,ch.getId,addrs(hostidx),ch.getLocalAddress.toString))
            val idx = hostidx + connidx * addrs.size

            lock.lock()

            try {
                if( channels(idx) == null ) {
                    val theConnId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
                    channels(idx) = ch
                    channelIds(idx) = theConnId
                }
            } finally {
                lock.unlock()
            }

            if( waitForAllConnected ) {

                if( connectedCount() == channels.size ) {
                    connected.set(true)
                }

            } else {
                connected.set(true)
            }

            val ladr = ch.getLocalAddress.toString
            if( NettyClient.localAddrsMap.contains(ladr) ) {
                log.warn("client addr duplicated, " + ladr )
            }	else {
                NettyClient.localAddrsMap.put(ladr,"1")
            }

            if( isSps ) {
                val buff = soc.generateReportSpsId()
                if( buff != null ) {
                    updateSpsId(buff,idx)
                    val reqBuf = ChannelBuffers.wrappedBuffer(buff);
                    ch.write(reqBuf);
                }
            }
        }

    }

    def selectChannel():String = {

        lock.lock()

        try {
            var i = 0

            while( i < channels.size ) {

                val channel = channels(i)
                val connId = channelIds(i)
                if( channel != null && channel.isOpen && !channelsMap.contains(connId) ) {
                    channelsMap.put(connId,channel)
                    return connId
                }

                i+=1
            }

            null

        } finally {
            lock.unlock()
        }

    }

    def nextChannelFromMap(sequence:Int,timeout:Int,connId:String) :Channel = {

        lock.lock()

        try {

            val ch = channelsMap.getOrElse(connId,null)
            if( ch == null ) return null

            if( ch.isOpen ) {
                val t = qte.newTimer(timeout,sequence,qteTimeoutFunctionId)
                val ti = new TimeoutInfo(sequence,connId,t)
                dataMap.put(sequence,ti)
                return ch
            } else {
                log.error("channel not opened, connId={}",connId)
                removeChannel(connId)
                return null
            }

        } finally {
            lock.unlock()
        }
    }

    def nextChannel(sequence:Int,timeout:Int,addr:String = null) : Tuple2[Channel,Int] = {

        lock.lock()

        try {

            var i = 0
            while(  i < channels.size ) {
                val ch = channels(nextIdx)
                val connId = channelIds(nextIdx)
                val chAddr = channelAddrs(nextIdx)
                if( ch != null ) { // && ch.isWritable

                    if( ch.isOpen ) {

                        var matchAddr = true
                        if( addr != null && chAddr != addr ) { // change to domain name match
                            matchAddr = false
                        }

                        if( matchAddr ) {
                            val t = qte.newTimer(timeout,sequence,qteTimeoutFunctionId)
                            val ti = new TimeoutInfo(sequence,connId,t)
                            dataMap.put(sequence,ti)

                            val d = (ch,nextIdx)
                            nextIdx += 1
                            if( nextIdx >= channels.size ) nextIdx = 0
                            return d

                        }

                        } else {
                            log.error("channel not opened, idx={}, connId={}",i,connId)
                            removeChannel(connId)
                        }

                }
                i+=1
                nextIdx += 1
                if( nextIdx >= channels.size ) nextIdx = 0
            }

            return (null,0)

        } finally {
            lock.unlock()
        }
    }

    def removeChannel(connId:String) :Unit = {

        if( shutdown.get()) {
            return
        }

        lock.lock()

        var idx = -1
        try {
            var i = 0

            while( idx == -1 && i < channels.size ) {
                val channel = channels(i)
                val theConnId = channelIds(i)
                if( channel != null && theConnId == connId ) {
                    channels(i) = null
                    channelIds(i) = null
                    idx = i
                }

                i+=1
            }

            channelsMap.remove(connId)

        } finally {
            lock.unlock()
        }

        if( idx != -1 ) {

            val hostidx = idx % addrs.size
            val connidx = idx / addrs.size

            timer.newTimeout( new TimerTask() {

                def run( timeout: Timeout) {
                    reconnect(hostidx,connidx)
                }

            }, reconnectInterval, TimeUnit.SECONDS)
        }

    }

    def send(sequence:Int, buff: ByteBuffer,timeout:Int) :Boolean = {

        val (ch,idx) = nextChannel(sequence,timeout)

        if( ch == null ) {
            return false
        }

        if( isSps ) updateSpsId(buff,idx)

        val reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);

        return true
    }


    def sendByAddr(sequence:Int, buff: ByteBuffer,timeout:Int,addr:String) :Boolean = {

        val (ch,idx) = nextChannel(sequence,timeout,addr)

        if( ch == null ) {
            return false
        }

        if( isSps ) updateSpsId(buff,idx)
        val reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);

        return true
    }

    def updateSpsId(buff:ByteBuffer,idx:Int) {
        val array = buff.array()
        val hostidx = idx % addrs.size
        val connidx = idx / addrs.size
        val spsId = guids(connidx)
        val spsIdArray = spsId.getBytes("ISO-8859-1")
        var i = 0
        while( i < spsIdArray.length ) {
            array(44+4+i) = spsIdArray(i) // start from the xhead (44), skip the xhead spsId head (4)
            i += 1
        }
    }

    def sendByConnId(sequence:Int, buff: ByteBuffer,timeout:Int,connId:String) :Boolean = {

        var ch = nextChannelFromMap(sequence,timeout,connId)

        if( ch == null ) {
            return false
        }

        val reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);

        return true
    }

    def sendResponse(sequence:Int, buff: ByteBuffer,connId:String) :Boolean = {

        var ch : Channel = null

        lock.lock()

        try {
            var i = 0

            while( i < channels.size && ch == null ) {
                val channel = channels(i)
                val theConnId = channelIds(i)
                if( channel != null && theConnId == connId ) {
                    ch = channels(i)
                }

                i+=1
            }
        } finally {
            lock.unlock()
        }

        if( ch == null ) {
            return false
        }

        val reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);

        return true
    }

    def parseIpPort(s:String):String = {

        val p = s.indexOf("/")

        if (p >= 0)
            s.substring(p + 1)
        else
            s
    }

    def onTimeout(data:Any):Unit = {

        val sequence = data.asInstanceOf[Int]

        val ti = dataMap.remove(sequence)
        if( ti != null ) {
            soc.timeoutError(sequence,ti.connId)
        } else {
            log.error("timeout but sequence not found, seq={}",sequence)
        }

    }

    def onReceive(buff:ByteBuffer,connId:String):Unit = {

        val (ok,sequence) = soc.receive(buff,connId)

        if( ok ) {

            //log.info("onReceive,seq="+sequence)

            val ti = dataMap.remove(sequence)
            if( ti != null ) {
                ti.timer.cancel()
            } else {
                log.warn("receive but sequence not found, seq={}",sequence)
            }

        }

    }

    def onNetworkError(connId:String):Unit = {

        removeChannel(connId)

        val seqs = new ArrayBufferInt()
        val i = dataMap.values().iterator
        while(i.hasNext()) {
            val info = i.next()
            if( info.connId == connId) {
                seqs += info.sequence
            }
        }

        for(sequence <- seqs) {
            val ti = dataMap.remove(sequence)
            if( ti != null ) {
                ti.timer.cancel()
                soc.networkError(sequence,connId)
            } else {
                log.error("network error but sequence not found, seq={}",sequence)
            }
        }

    }

    def messageReceived(ctx:ChannelHandlerContext, e:MessageEvent): Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String]
        val buf = e.getMessage().asInstanceOf[ChannelBuffer]
        val bb = buf.toByteBuffer()
        onReceive(bb,connId)
    }

    def channelConnected(ctx:ChannelHandlerContext, e:ChannelStateEvent):Unit = {
        val ch = e.getChannel
        val connId = parseIpPort(ch.getRemoteAddress.toString) + ":" + ch.getId
        ctx.setAttachment(connId);
    }

    def channelDisconnected(ctx:ChannelHandlerContext, e:ChannelStateEvent):Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String]
        onNetworkError(connId)
        log.info("channelDisconnected id={}",connId)
    }

    def exceptionCaught(ctx:ChannelHandlerContext, e: ExceptionEvent) :Unit = {
        val ch = e.getChannel
        val connId = ctx.getAttachment().asInstanceOf[String];
        log.error("exceptionCaught connId={},e={}",connId,e)
        if( ch.isOpen )
            ch.close()
    }

    def channelIdle(ctx:ChannelHandlerContext, e:IdleStateEvent) : Unit = {
        val ch = e.getChannel
        val buff = soc.generatePing()
        val reqBuf = ChannelBuffers.wrappedBuffer(buff);
        ch.write(reqBuf);
    }

    class PipelineFactory extends Object with ChannelPipelineFactory {

        def getPipeline() : ChannelPipeline =  {
            val pipeline = Channels.pipeline();
            val decoder = new LengthFieldBasedFrameDecoder(maxPackageSize, 4, 4, -8, 0);
            pipeline.addLast("timeout", new IdleStateHandler(timer, 0, 0, pingInterval / 1000));
            pipeline.addLast("decoder", decoder);
            pipeline.addLast("handler", channelHandler);
            pipeline;
        }
    }

    class ChannelHandler(val nettyClient: NettyClient) extends IdleStateAwareChannelHandler with Logging  {

        override def messageReceived(ctx:ChannelHandlerContext, e:MessageEvent): Unit = {
            nettyClient.messageReceived(ctx,e)
        }

        override def channelIdle(ctx:ChannelHandlerContext, e:IdleStateEvent) : Unit = {
            nettyClient.channelIdle(ctx,e)
        }

        override def exceptionCaught(ctx:ChannelHandlerContext, e: ExceptionEvent) :Unit = {
            nettyClient.exceptionCaught(ctx,e)
        }

        override def channelConnected(ctx:ChannelHandlerContext, e:ChannelStateEvent):Unit = {
            nettyClient.channelConnected(ctx,e)
        }

        override def channelDisconnected(ctx:ChannelHandlerContext, e:ChannelStateEvent):Unit = {
            nettyClient.channelDisconnected(ctx,e)
        }

    }

    class TimeoutInfo(val sequence:Int, val connId:String, val timer: QuickTimer)

}


