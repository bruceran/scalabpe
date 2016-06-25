package jvmdbbroker.core

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.buffer._;
import org.jboss.netty.channel._;
import org.jboss.netty.handler.timeout._;
import org.jboss.netty.bootstrap._;
import org.jboss.netty.channel.group._;
import org.jboss.netty.channel.socket.nio._;
import org.jboss.netty.handler.codec.http._;
import org.jboss.netty.util._;
import org.jboss.netty.handler.ssl.SslHandler;

// used by netty
trait HttpClient4Netty {
    def receive(sequence:Int,httpRes:HttpResponse):Unit;
    def networkError(sequence:Int):Unit;
    def timeoutError(sequence:Int):Unit;
}

class NettyHttpClientHandler(val nettyHttpClient: NettyHttpClient) extends SimpleChannelUpstreamHandler  with Logging {

    override def messageReceived(ctx:ChannelHandlerContext, e:MessageEvent): Unit = {
        nettyHttpClient.messageReceived(ctx,e)
    }

    override def exceptionCaught(ctx:ChannelHandlerContext, e: ExceptionEvent) :Unit = {
      nettyHttpClient.exceptionCaught(ctx,e)
    }

    override def channelDisconnected(ctx:ChannelHandlerContext, e:ChannelStateEvent):Unit = {
      nettyHttpClient.channelDisconnected(ctx,e)
    }

}

class NettyHttpClientTimeoutInfo(val sequence:Int, val channel:Channel, val timer: QuickTimer)

object NettyHttpClient {
  val count = new AtomicInteger(1)
}

class NettyHttpClient(
                  val httpClient: HttpClient4Netty,
                  val connectTimeout :Int = 15000,
                  val timerInterval :Int = 100 )
                  extends Logging with Dumpable {

  var factory : NioClientSocketChannelFactory = _
  val pipelineFactory = new NettyPipelineFactory()
  var sslPipelineFactory : SslNettyPipelineFactory = null
  var nettyHttpClientHandler : NettyHttpClientHandler = _

  val qte = new QuickTimerEngine(onTimeout,timerInterval)

  val dataMap = new ConcurrentHashMap[Int,NettyHttpClientTimeoutInfo]() // key is sequence
  val connMap = new ConcurrentHashMap[Int,NettyHttpClientTimeoutInfo]() // key is channel.getId

  val bossThreadFactory = new NamedThreadFactory("httpclientboss"+NettyHttpClient.count.getAndIncrement())
  val workThreadFactory = new NamedThreadFactory("httpclientwork"+NettyHttpClient.count.getAndIncrement())

  var bossExecutor:ThreadPoolExecutor = _
  var workerExecutor:ThreadPoolExecutor = _

  init

  def dump() {

      val buff = new StringBuilder

      buff.append("dataMap.size=").append(dataMap.size).append(",")
      buff.append("connMap.size=").append(connMap.size).append(",")

      buff.append("bossExecutor.getPoolSize=").append(bossExecutor.getPoolSize).append(",")
      buff.append("bossExecutor.getQueue.size=").append(bossExecutor.getQueue.size).append(",")
      buff.append("workerExecutor.getPoolSize=").append(workerExecutor.getPoolSize).append(",")
      buff.append("workerExecutor.getQueue.size=").append(workerExecutor.getQueue.size).append(",")

      log.info(buff.toString)

      qte.dump
  }

  def close() : Unit = {

      if (factory != null) {

          // TODO close existed channels

          log.info("stopping netty http client")
          factory.releaseExternalResources()
          factory = null
      }

      qte.close()

      log.info("netty http client stopped")
  }

  def init() : Unit  = {

      nettyHttpClientHandler = new NettyHttpClientHandler(this)

      // without this line, the thread name of netty will not be changed
      ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT); // or PROPOSED

      bossExecutor = Executors.newCachedThreadPool(bossThreadFactory).asInstanceOf[ThreadPoolExecutor]
      workerExecutor = Executors.newCachedThreadPool(workThreadFactory).asInstanceOf[ThreadPoolExecutor]

      factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor)


      log.info("netty http client started")
  }

  def send(sequence:Int, addr:String, httpReq:HttpRequest, timeout:Int) : Unit = {

    var ss = addr.split(":")
    var host = ss(0)
    var port = if( ss.size >= 2 ) ss(1).toInt else 80

    val ssl = port >= 443 && port <= 449

    val bootstrap = new ClientBootstrap(factory);
    if( ssl ) {

      if( SslContextFactory.CLIENT_CONTEXT == null ) {
        httpClient.networkError(sequence)
        return
      }
      if( sslPipelineFactory == null )
          sslPipelineFactory = new SslNettyPipelineFactory()
      bootstrap.setPipelineFactory(sslPipelineFactory);

    } else {

      bootstrap.setPipelineFactory(pipelineFactory);

    }
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);

    val t = if( timeout < connectTimeout ) timeout else connectTimeout
    bootstrap.setOption("connectTimeoutMillis", t);

    val future = bootstrap.connect(new InetSocketAddress(host,port));
    future.addListener( new ChannelFutureListener() {
      def operationComplete(future: ChannelFuture ) {
        onConnectCompleted(future,sequence,addr,httpReq,timeout)
      }
    } )

  }

  def onConnectCompleted(f: ChannelFuture, sequence:Int, addr:String, httpReq:HttpRequest, timeout:Int) : Unit = {

      if (f.isCancelled()) {
         log.error("onConnectCompleted f.isCancelled should not be called")
     } else if (!f.isSuccess()) {
         log.error("connect failed, addr={},e={}",addr,f.getCause.getMessage)
         httpClient.networkError(sequence)
     } else {
         // log.debug(addr+" connected");
         val ch = f.getChannel

         val t = qte.newTimer(timeout,sequence)
         val ti = new NettyHttpClientTimeoutInfo(sequence,ch,t)
         dataMap.put(sequence,ti)
         connMap.put(ch.getId,ti)

         ch.write(httpReq);
     }

  }

  def onTimeout(data:Any):Unit = {

     val sequence = data.asInstanceOf[Int]

     val ti = dataMap.remove(sequence)
     if( ti != null ) {
       connMap.remove(ti.channel.getId)
       httpClient.timeoutError(sequence)
       ti.channel.close()
     } else {
       log.error("timeout but sequence not found, seq={}",sequence)
     }

  }

  def messageReceived(ctx:ChannelHandlerContext, e:MessageEvent): Unit = {
      val ch = e.getChannel

      val ti = connMap.remove(ch.getId)
      if( ti == null ) return
      dataMap.remove(ti.sequence)
      ti.timer.cancel()

      val httpRes = e.getMessage().asInstanceOf[HttpResponse]

      httpClient.receive(ti.sequence,httpRes)

      ch.close()
  }

  def exceptionCaught(ctx:ChannelHandlerContext, e: ExceptionEvent) :Unit = {
      val ch = e.getChannel

      val ti = connMap.remove(ch.getId)
      if( ti == null ) return
      dataMap.remove(ti.sequence)
      ti.timer.cancel()

      val remoteAddr = ch.getRemoteAddress.toString
      log.error("exceptionCaught addr={},e={}",remoteAddr,e)

      httpClient.networkError(ti.sequence)

      ch.close()
  }

  def channelDisconnected(ctx:ChannelHandlerContext, e:ChannelStateEvent):Unit = {
    val ch = e.getChannel
    val remoteAddr = ch.getRemoteAddress.toString
    // log.debug(remoteAddr+" disconnected");

    // remove dataMap

    val ti = connMap.remove(ch.getId)
    if( ti == null ) return
    dataMap.remove(ti.sequence)
    ti.timer.cancel()

  }

  class NettyPipelineFactory extends Object with ChannelPipelineFactory {

      def getPipeline() : ChannelPipeline =  {
          val pipeline = Channels.pipeline();
          pipeline.addLast("decoder", new HttpResponseDecoder());
          pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
          pipeline.addLast("encoder", new HttpRequestEncoder());
          pipeline.addLast("handler", nettyHttpClientHandler);
          pipeline;
      }
  }

  class SslNettyPipelineFactory extends Object with ChannelPipelineFactory {

      def getPipeline() : ChannelPipeline =  {
          val pipeline = Channels.pipeline();

          val engine = SslContextFactory.CLIENT_CONTEXT.createSSLEngine();
          engine.setUseClientMode(true);

          pipeline.addLast("ssl", new SslHandler(engine));

          pipeline.addLast("decoder", new HttpResponseDecoder());
          pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
          pipeline.addLast("encoder", new HttpRequestEncoder());
          pipeline.addLast("handler", nettyHttpClientHandler);
          pipeline;
      }
  }

}

import javax.net.ssl._
import java.security._
import java.security.cert._

object SslContextFactory {

    val PROTOCOL = "TLS"
    var CLIENT_CONTEXT: SSLContext = _

    init

    def init() {

        var algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
        if (algorithm == null) {
            algorithm = "SunX509"
        }

        try {
            val clientContext = SSLContext.getInstance(PROTOCOL);
            clientContext.init(null, SslTrustManagerFactory.getTrustManagers(), null);
            CLIENT_CONTEXT = clientContext;
        } catch {
          case e:Exception =>
            throw new Error(
                    "Failed to initialize the client-side SSLContext", e);
        }

    }

}

object SslTrustManagerFactory extends Logging {

    val DUMMY_TRUST_MANAGER = new X509TrustManager() {

        override def getAcceptedIssuers() : Array[X509Certificate] = Array[X509Certificate]()
        override def checkClientTrusted(chain: Array[X509Certificate], authType: String) {}

        /*

        ssl certificate from alipay:

        SERVER CERTIFICATE: CN=*.alipay.com, OU=Terms of use at www.verisign.com/rpa (c)05,
        OU=Operations Department, O="Alipay.com Co.,Ltd", L=HANGZHOU, ST=ZHEJIANG, C=CN

        ssl certificate from google:

        SERVER CERTIFICATE: CN=www.google.com,
        O=Google Inc, L=Mountain View, ST=California, C=US


        */

        override def checkServerTrusted(chain: Array[X509Certificate], authType: String) {
            // TODO Always trust any server certificate, should do something.
            if( log.isDebugEnabled() ) {
                log.debug("SERVER CERTIFICATE: " + chain(0).getSubjectDN());
            }
        }
    };

    def getTrustManagers() : Array[TrustManager] = {
        Array( DUMMY_TRUST_MANAGER )
    }

}
