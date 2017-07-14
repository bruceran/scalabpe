package scalabpe.plugin.kafka

import java.io._
import java.util._
import java.util.concurrent._
import scala.xml._
import scala.collection.mutable.{ArrayBuffer,HashMap,HashSet}
import java.util.concurrent.atomic.{AtomicBoolean,AtomicInteger}
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer._

import com.sdo.billing.queue._
import com.sdo.billing.queue.impl._

import scalabpe.core._

object KafkaProducerActor{
    val localDirs = new HashSet[String]()
}

class KafkaProducerActor(val router:Router,val cfgNode: Node) extends Actor with Logging with Closable  with SelfCheckLike with Dumpable {

    var serviceIds : String = _

    var threadNum = 1
    val queueSize = 10000
    var threadFactory : ThreadFactory = _
    var pool : ThreadPoolExecutor = _

    var client : KafkaProducerClient = _


    init

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        var s = (cfgNode \ "@threadNum").text
        if( s != "" ) threadNum = s.toInt

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory("kafka"+firstServiceId)
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize),threadFactory)
        pool.prestartAllCoreThreads()

        client = new KafkaProducerClient(router,cfgNode,this)

        log.info("KafkaProducerActor started {}",serviceIds)
    }

    def close() {

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5,TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if( t2 - t1 > 100 )
            log.warn("KafkaProducerActor long time to shutdown pool, ts={}",t2-t1)

        client.close()

        log.info("KafkaProducerActor closed {} ",serviceIds)
    }

    override def receive(v:Any) :Unit = {
        v match {

            case req: Request =>

                try {

                    pool.execute( new Runnable() {
                        def run() {

                            try {
                                onReceive(req)
                            } catch {
                                case e:Exception =>
                                    log.error("MqActor exception req={}",req,e)
                            }

                        }
                    } )

                } catch {
                    case e: RejectedExecutionException =>
                        replyError(ResultCodes.SERVICE_FULL,req)
                        log.error("KafkaProducerActor queue is full, serviceIds={}",serviceIds)
                }

            case _ =>

                log.error("unknown msg")

        }
    }

    def onReceive(v:Any)  {

        v match {

            case req: Request =>

                try {
                    val ok = client.send(req)
                    if(ok)
                        replyOk(req)
                    else
                        replyError(ResultCodes.TLV_ENCODE_ERROR,req)
                } catch {
                    case e:Exception =>
                        log.error("mq send error exception=%s, str=%s".format(e.getMessage,req.toString))
                        replyError(ResultCodes.SERVICE_INTERNALERROR,req)
                }

            case _ =>

                log.error("unknown msg")

        }
    }

    def replyOk(req : Request) {
        val res = new Response (0,new HashMapStringAny(),req)
        router.reply(new RequestResponseInfo(req,res))
    }

    def replyError(code:Int, req : Request) {
        val res = new Response (code,new HashMapStringAny(),req)
        router.reply(new RequestResponseInfo(req,res))
    }

    def dump() {

        val buff = new StringBuilder
        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")
        log.info(buff.toString)

        client.dump()
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = new ArrayBuffer[SelfCheckResult]()
        val r = client.selfcheck()
        buff ++= r
        buff
    }
}

class KafkaProducerClient(val router:Router,val cfgNode: Node, val actor:KafkaProducerActor) extends Logging  {

    var producer: KafkaProducer[String,String] = _

    val FLAG_SEND = 0
    val FLAG_RETRY = 1

    var brokerUrl : String = _
    var batchSize = "100"

    var persistQueueManager : PersistQueueManagerImpl = _
    val sendQueueName = "kafkasend"
    val retryQueueName = "kafkaretry"
    var sendThread : Thread = _
    var retryThread : Thread = _

    var maxRetryTimes = 12000
    var retryInterval = 5000
    var retryConcurrentNum = 200

    val lock = new ReentrantLock(false)
    val sending = new AtomicInteger(0)
    val waitToSend = lock.newCondition()

    val hasIOException = new AtomicBoolean()

    init

    def init() {

        var s = (cfgNode \ "BrokerUrl").text
        if( s == "" )
            throw new Exception("KafkaProducerActor BrokerUrl not configed")
        brokerUrl = s

        s = (cfgNode \ "@batchSize").text
        if( s != "" ) batchSize = s
        s = (cfgNode \ "@maxRetryTimes").text
        if( s != "" ) maxRetryTimes = s.toInt
        s = (cfgNode \ "@retryInterval").text
        if( s != "" ) retryInterval = s.toInt
        s = (cfgNode \ "@retryConcurrentNum").text
        if( s != "" ) retryConcurrentNum = s.toInt

        var localDir = (cfgNode \ "LocalDir").text
        if( localDir == "" ) {
            localDir = Router.dataDir + File.separator + "kafkaproducer"
        }

        if( KafkaProducerActor.localDirs.contains(localDir) ) {
            throw new RuntimeException("KafkaProducerActor localDir cannot be the same, the default is data/kafkaproducer")
        }

        KafkaProducerActor.localDirs.add(localDir)
        var dataDir = ""
        if (localDir.startsWith("/")) dataDir = localDir
        else dataDir = router.rootDir + File.separator + localDir
        new File(dataDir).mkdirs()
        persistQueueManager = new PersistQueueManagerImpl()
        persistQueueManager.setDataDir(dataDir)
        persistQueueManager.init()

        val props = new Properties();
        props.put("bootstrap.servers", brokerUrl);
        props.put("batch.size", batchSize);

        // 在kafka 0.8版本下
        // 如果kafka启动时无法连接，metadata无法获取，则send()方法会阻塞，metadata.fetch.timeout.ms参数可控制阻塞时间 
        // 如果启动后kafka无法连接, send()方法不会阻塞，但是callback不会回来，后台会每隔一秒尝试连接一次服务器; 这时调用close消息会丢失
        //props.put("metadata.fetch.timeout.ms", 15000);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        val configList = (cfgNode \ "Config")
        for( t <- configList ) {
            val key = (t \ "@key").text
            val value = t.text
            if( !props.contains(key ) )
                props.put(key,value)
        }

        producer = new KafkaProducer(props)

        sendThread = new Thread("kafkasendthread") {
            override def run() {
                send(FLAG_SEND)
            }
        }
        sendThread.start()
        retryThread = new Thread("kafkaretrythread") {
            override def run() {
                send(FLAG_RETRY)
            }
        }
        retryThread.start()

        log.info("kafka producer client started, brokerUrl=%s".format(brokerUrl))
    }

    def close() {

        sendThread.interrupt()
        sendThread.join()
        retryThread.interrupt()
        retryThread.join()

        producer.close()

        if( persistQueueManager != null ) {
            persistQueueManager.close()
            persistQueueManager = null
        }

        log.info("kafka producer client closed, brokerUrl=%s".format(brokerUrl))
    }

    def send(req:Request):Boolean = {
        val topic = req.s("topic","")
        if( topic == "") return false
        val key = req.s("key")
        var value = req.s("value","")

        val map = HashMapStringAny(
            "requestId" -> req.requestId,
            "topic" -> topic,
            "key" -> key,
            "value" -> value,
            "retry" -> 0,
            "timestamp" -> System.currentTimeMillis
        )
        val str = JsonCodec.mkString(map)
        try {
            val queue = persistQueueManager.getQueue(sendQueueName) 
            queue.put(str)
            hasIOException.set(false)
            return true
        } catch {
            case e:Throwable =>
                // 只可能本地文件系统出问题，跳过本地持久化队列直接发送给kafka
                hasIOException.set(true)
                val msg = new ProducerRecord(topic,key,value)
                producer.send(msg)
                return true
        }
    }

    def send(flag:Int) {

        val queueName = if( flag == FLAG_SEND ) sendQueueName else retryQueueName 
        val queue = persistQueueManager.getQueue(queueName) 

        while(true) {

            try {
                val idx = queue.get()
                if( idx == -1 ) {
                    return
                }
                val str = queue.getString(idx)

                val data = JsonCodec.parseObject(str)
                if( data != null)
                    sendData(flag,queue,idx,data)
                else 
                    queue.commit(idx) // ignore the data

            } catch {
                case e: InterruptedException =>
                    return
                case e : Exception =>
                    log.error("exception in retry kafka producer data {}",e.getMessage)
                    Thread.sleep(5000)
            }
        }
    }

    def sendData(flag:Int,queue:PersistQueue,idx:Long,data:HashMapStringAny) {

        var retry = data.i("retry")
        if( flag == FLAG_RETRY ) {
            if( retry > maxRetryTimes ) {
                queue.commit(idx)
                return
            }

            val timestamp = data.s("timestamp").toLong
            val now = System.currentTimeMillis
            val left = now - timestamp 
            if( left + 20 < retryInterval )
                Thread.sleep( retryInterval - left )
        }

        val requestId = data.s("requestId","")
        val topic = data.s("topic","")
        val key = data.s("key")
        var value = data.s("value","")

        val callback = new Callback() {
            def onCompletion(metadata:RecordMetadata, e:Exception) {
                if( e == null ) {
                    if( log.isDebugEnabled ) {
                        log.debug("send success, requestId=%s,topic=%s,partition=%d,key=%s,value=%s".format(requestId,topic,metadata.partition,key,value))
                    }
                } else {
                    log.error("send failed, requestId=%s,topic=%s,key=%s,value=%s,retry=%d".format(requestId,topic,key,value,retry),e)
                    retry += 1 
                    data.put("retry",retry)
                    data.put("timestamp",System.currentTimeMillis.toString)
                    val str = JsonCodec.mkString(data)
                    try {
                        val retryQueue = persistQueueManager.getQueue(retryQueueName) 
                        retryQueue.put(str)
                    } catch {
                        case e:Throwable =>
                            log.error("cannot write data to retry queue,, requestId=%s,topic=%s,key=%s,value=%s".format(requestId,topic,key,value))
                    }
                }
                queue.commit(idx)
                if( flag == FLAG_RETRY ) {
                    sending.decrementAndGet()
                    lock.lock()
                    try {
                        waitToSend.signal()
                    } finally {
                        lock.unlock()
                    }
                }

            }
        }

        if( log.isDebugEnabled() ) {
            log.debug("sending ... requestId=%s,topic=%s,key=%s,value=%s".format(requestId,topic,key,value))
        }

        val msg = new ProducerRecord(topic,key,value)
        if( flag == FLAG_SEND ) {
            producer.send(msg,callback)
            //log.debug("send called, requestId=%s,topic=%s,key=%s,value=%s".format(requestId,topic,key,value))
        } else {
            val cnt = sending.incrementAndGet()
            producer.send(msg,callback)
            if( cnt >= retryConcurrentNum ) { // 控制同时发出的消息数，超过此数量，需等待callback后才继续发
                lock.lock()
                try {
                    waitToSend.await()
                } finally {
                    lock.unlock()
                }
            }
        }
    }

    def dump() {
        val buff1 = new StringBuilder
        val buff2 = new StringBuilder

        buff1.append("queue size ")
        buff2.append("queue cacheSize ")
        val queueNames = persistQueueManager.getQueueNames
        for( i <- 0 until queueNames.size ) {
            val queue = persistQueueManager.getQueue(queueNames.get(i))
            buff1.append(queueNames.get(i)).append("=").append(queue.size).append(",")
            buff2.append(queueNames.get(i)).append("=").append(queue.cacheSize).append(",")
        }
        log.info(buff1.toString)
        log.info(buff2.toString)
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = new ArrayBuffer[SelfCheckResult]()

        var ioErrorId = 65301007

        if( hasIOException.get() ) {
            val msg = "local persistqueue has io error"
            buff += new SelfCheckResult("SCALABPE.IO",ioErrorId,true,msg)
        }

        buff

    }

}

