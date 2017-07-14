package scalabpe.plugin.kafka

import java.io.{File,StringWriter}
import java.util._
import java.util.concurrent._
import java.util.concurrent.locks.{ReentrantLock,Condition}
import java.util.concurrent.atomic.{AtomicBoolean,AtomicInteger}
import scala.xml._
import scala.collection.mutable.{ArrayBuffer,HashMap,HashSet}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.errors._

import com.sdo.billing.queue._
import com.sdo.billing.queue.impl._

import scalabpe.core._

object KafkaConsumerBeanV2 {
    val localDirs = new HashSet[String]()
}

class KafkaConsumerBeanV2(val router:Router,val cfgNode: Node)
extends Logging with Actor with Bean with Closable with SelfCheckLike with Dumpable {

    var bootstrap : String = _
    var groupId = "scalabpe"
    var retryInterval = 5000
    var batchSize = 10

    val receiverMap = new HashMap[String,Tuple2[Int,Int]]()
    var receiverLockMap = new java.util.HashMap[String,ReentrantLock]()
    var receiverConditionMap = new java.util.HashMap[String,Condition]()
    var receiverThreadMap = new HashMap[String,Thread]()
    var consumerThreadMap = new HashMap[String,Thread]()
    var consumerMap = new HashMap[String,KafkaConsumer[String, String]]()

    var persistQueueManager : PersistQueueManagerImpl = _

    val requestIdMap = new ConcurrentHashMap[String,Request]()
    val requestIdResultMap = new ConcurrentHashMap[String,InvokeResult]()
    val sequence = new AtomicInteger(1)

    var threadNum = 1
    val queueSize = 10000
    var threadFactory : ThreadFactory = _
    var pool : ThreadPoolExecutor = _

    val hasIOException = new AtomicBoolean()
    var shutdown = new AtomicBoolean(false)

    init

    def init() {

        var s = (cfgNode \ "BootstrapServers").text
        if( s == "" )
            throw new Exception("KafkaConsumerBeanV2 BootstrapServers not configed")
        bootstrap = s

        s = (cfgNode \ "@groupId").text
        if( s != "" )
            groupId = s

        s = (cfgNode \ "@retryInterval").text
        if( s != "" )
            retryInterval = s.toInt

        s = (cfgNode \ "@batchSize").text
        if( s != "" )
            batchSize = s.toInt

        var localDir = (cfgNode \ "LocalDir").text
        if( localDir == "" ) {
            localDir = Router.dataDir + File.separator + "kafkaconsumerv2"
        }

        if( KafkaConsumerBeanV2.localDirs.contains(localDir) ) {
            throw new RuntimeException("KafkaConsumerBeanV2.localDirs cannot be the same, the default is data/kafkaproducer")
        }

        KafkaConsumerBeanV2.localDirs.add(localDir)
        var dataDir = ""
        if (localDir.startsWith("/")) dataDir = localDir
        else dataDir = router.rootDir + File.separator + localDir
        new File(dataDir).mkdirs()

        persistQueueManager = new PersistQueueManagerImpl()
        persistQueueManager.setDataDir(dataDir)
        persistQueueManager.init()

        threadFactory = new NamedThreadFactory("kafkaconsumerv2")
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize),threadFactory)
        pool.prestartAllCoreThreads()

        val topicList = (cfgNode \ "Topic")
        for( t <- topicList ) {
            val name = (t \ "@name").text
            val receiver = (t \ "@receiver").text
            val ss = receiver.split("\\.")
            if( ss.size < 2 ) 
                throw new Exception("KafkaConsumerBeanV2 topic receiver not correct")

            receiverMap.put(name,new Tuple2(ss(0).toInt,ss(1).toInt))

            val lock = new ReentrantLock(false)
            val cond = lock.newCondition()
            receiverLockMap.put(name,lock)
            receiverConditionMap.put(name,cond)

            val ct = new Thread() {
                override def run() {
                    consumeData(name)
                }
            }
            consumerThreadMap.put(name,ct)
            ct.start()

            val rt = new Thread() {
                override def run() {
                    sendData(name)
                }
            }
            receiverThreadMap.put(name,rt)
            rt.start()
        }

        log.info("kafka consumer started")
    }

    def sendData(topic:String) {

        val queue = persistQueueManager.getQueue(topic) 

        while(true) {

            try {
                val idx = queue.get()
                if( idx == -1 ) {
                    return
                }
                val str = queue.getString(idx)

                val p = str.indexOf("\t")
                if( p < 0 ) {
                    queue.commit(idx)
                } else {
                    var key = str.substring(0,p)
                    if( key == "_null_") key = null
                    val value = str.substring(p+1)
                    var ok = false 
                    do {
                        ok = sendData(topic,key,value)
                        if(!ok) {
                            Thread.sleep(retryInterval)
                        }
                    } while(!ok)
                    queue.commit(idx)
                }

            } catch {
                case e: InterruptedException =>
                    return
                case e : Exception =>
                    log.error("exception in retry kafka consumer data {}",e.getMessage)
                    Thread.sleep(retryInterval)
            }
        }
    }

    def sendData(topic:String,key:String,value:String):Boolean = {
    
        val (serviceId,msgId) = receiverMap.getOrElse(topic,new Tuple2(0,0))
        if( serviceId == 0 || msgId == 0 ) return true

        val body = HashMapStringAny(
            "topic" -> topic,
            "key" -> key,
            "value" -> value)

        var requestId = "KC"+RequestIdGenerator.nextId()

        val req = new Request (
            requestId,
            "kafkaconsumerv2:0",
            sequence.getAndIncrement(),
            1,
            serviceId,
            msgId,
            new HashMapStringAny(),
            body,
            this
        )

        requestIdMap.put(requestId,req)

        val lock = receiverLockMap.get(topic)
        val cond = receiverConditionMap.get(topic)

        lock.lock()
        try {
            router.send(req)
            cond.await()
        } finally {
            lock.unlock()
        }
        val res = requestIdResultMap.remove(requestId)
        res.code == 0
    }

    override def receive(v:Any) :Unit = {

        try {

            pool.execute( new Runnable() {
                def run() {

                    try {
                        onReceive(v)
                    } catch {
                        case e:Exception =>
                            log.error(getClass.getName+" exception req={}",v.toString,e)
                    }

                }
            } )

        } catch {
            case e: RejectedExecutionException =>
                log.error(getClass.getName+" queue is full")
                onReceive(v)
        }

    }

    def onReceive(v:Any) :Unit = {

        v match {

            case res : InvokeResult =>
                val req = requestIdMap.remove(res.requestId)
                if( req == null ) return

                val topic = req.s("topic")
                val lock = receiverLockMap.get(topic)
                val cond = receiverConditionMap.get(topic)

                lock.lock()
                try {
                    requestIdResultMap.put(res.requestId,res)
                    cond.signal()
                } finally {
                    lock.unlock()
                }

            case _ =>
                log.error("unknown msg")
        }

    }

    def consumeData(topic:String) {
        val props = new Properties()
        props.put("bootstrap.servers", bootstrap)
        props.put("group.id", groupId)

        val configList = (cfgNode \ "Config")
        for( t <- configList ) {
            val key = (t \ "@key").text
            val value = t.text
            if( !props.contains(key ) )
                props.put(key,value)
        }

        props.put("enable.auto.commit", "false")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        var consumer : KafkaConsumer[String, String] = null
        while(consumer == null) {
            try {
                consumer = new KafkaConsumer[String,String](props)
                consumer.subscribe(Arrays.asList(topic))
            } catch {
                case e:InterruptedException =>
                    if( shutdown.get() )
                        return
                case e:Throwable =>
                    try {
                        log.error("waiting to connect to bootstrap, topic="+topic,e)
                        Thread.sleep(1000)
                        if( shutdown.get() )
                            return
                    } catch { case e:Throwable =>
                            return
                    }
            }
        }
        log.info("connected to kafka, topic="+topic)
        consumerMap.put(topic,consumer)

        val queue = persistQueueManager.getQueue(topic)
        try {
            while (!shutdown.get()) {
                val records = consumer.poll(batchSize);
                val it = records.iterator()
                while( it.hasNext() ) {
                  val record = it.next()
                  val offset = record.offset()
                  val key0 = record.key()
                  val key = if( key0 == null ) "_null_" else key0
                  val value = record.value()

                  var ok = false
                  do {
                      try {
                          queue.put(key+"\t"+value)
                          ok = true
                          hasIOException.set(false)
                      } catch {
                        case e:WakeupException =>
                            throw e
                        case e:InterruptedException =>
                            throw e
                        case e:Throwable =>
                            log.error("cannot save message to local queue, waiting to retry")
                            hasIOException.set(true)
                            Thread.sleep(5000)
                      }
                  } while( !ok )
                  consumer.commitSync()
                }
            }
        } catch {
            case e:Throwable =>
        } finally {
            consumer.close();
            log.info("consumer closed for topic, topic="+topic)
        }
    }

    def close() {
        shutdown.set(true)

        for( (topic,consumer) <- consumerMap ) {
            consumer.wakeup();
        }

        for( (topic,t) <- consumerThreadMap ) {
            t.join();
        }

        for( (topic,t) <- receiverThreadMap ) {
            t.interrupt();
        }
        for( (topic,t) <- receiverThreadMap ) {
            t.join();
        }

        val t1 = System.currentTimeMillis
        pool.shutdown()
        pool.awaitTermination(5,TimeUnit.SECONDS)
        val t2 = System.currentTimeMillis
        if( t2 - t1 > 100 )
            log.warn(getClass.getName+" long time to shutdown pool, ts={}",t2-t1)

        if( persistQueueManager != null ) {
            persistQueueManager.close()
            persistQueueManager = null
        }

        log.info("kafka consumer closed")
    }

    def dump() {
        val buff = new StringBuilder
        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")
        buff.append("consumerThreadMap.size=").append(consumerThreadMap.size).append(",")
        buff.append("receiverThreadMap.size=").append(receiverThreadMap.size).append(",")
        buff.append("requestIdMap.size=").append(requestIdMap.size).append(",")
        buff.append("requestIdResultMap.size=").append(requestIdResultMap.size)
        log.info(buff.toString)

        dumpPersistManager()
    }

    def dumpPersistManager() {
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

