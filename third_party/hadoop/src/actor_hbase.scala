package jvmdbbroker.plugin

import java.util._
import java.util.concurrent._
import java.util.concurrent.locks._
import java.util.concurrent.atomic._
import scala.xml._
import scala.collection.mutable.{ArrayBuffer,HashMap,HashSet}
import HbaseClient._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._

import jvmdbbroker.core._

class HbaseActor(val router:Router,val cfgNode: Node) 
   extends Actor with Logging with Closable with SelfCheckLike with Dumpable  {

    var serviceIds: String = _

    val queueSize = 100000
    var maxThreadNum = 4
    var threadFactory : ThreadFactory = _
    var pool : ThreadPoolExecutor = _

    var hbase: HbaseClient = _

    init

    def dump() {

        log.info("--- serviceIds="+serviceIds)

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        log.info(buff.toString)

        hbase.dump
    }

    def init() {

      serviceIds = (cfgNode \ "ServiceId").text

      var s = (cfgNode \ "@threadNum").text
      if( s != "" ) maxThreadNum = s.toInt

      val firstServiceId = serviceIds.split(",")(0)
      threadFactory = new NamedThreadFactory("hbase"+firstServiceId)
      pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize),threadFactory)
      pool.prestartAllCoreThreads()

      hbase = new HbaseClient(serviceIds,cfgNode,router,this)

      log.info("HbaseActor started {}",serviceIds)
    }

    def close() {

      val t1 = System.currentTimeMillis

      pool.shutdown()

      pool.awaitTermination(5,TimeUnit.SECONDS)

      val t2 = System.currentTimeMillis
      if( t2 - t1 > 100 )
        log.warn("HbaseActor long time to shutdown pool, ts={}",t2-t1)

      hbase.close()
      log.info("HbaseActor stopped {}",serviceIds)
    }

    override def receive(v:Any) :Unit = {

      try {
        pool.execute( new Runnable() {
                        def run() {
                            try {
                              onReceive(v)
                            } catch {
                              case e:Exception =>
                                log.error("soc exception v={}",v,e)
                            }
                        }
                      })
        } catch {
          case e: RejectedExecutionException =>
            if( v.isInstanceOf[Request])
                hbase.reply(v.asInstanceOf[Request],ResultCodes.SERVICE_FULL)
            log.error("hbase queue is full, serviceIds={}",serviceIds)
        }
    }

    def onReceive(v:Any) :Unit = {

        v match {

            case req: Request =>

                try {
                 hbase.send(req)
                } catch {
                    case e:Throwable =>
                        val res = new Response(HBASE_ERROR,new HashMapStringAny(),req)
                        reply(new RequestResponseInfo(req,res))
                }
            case _ =>

              log.error("unknown msg")

      }
  }

  def reply(reqResInfo: RequestResponseInfo):Unit = {
    router.reply(reqResInfo)
  }

  def selfcheck() : ArrayBuffer[SelfCheckResult] = {
    val buff = hbase.selfcheck()
    buff
  }

}

object HbaseClient {

    val ACTION_UNKNOWN = -1
    val ACTION_GET = 1
    val ACTION_PUT = 2
    val ACTION_DELETE = 3

    val EMPTY_STRINGMAP = new HashMapStringString()

    val ACTION = "action"
    val TABLE_NAME = "tableName"
    val COLUMN_FAMILY = "columnFamily"
    val ROW_KEY = "rowKey"
    val IS_DYNAMIC = "isDynamic"

    val HBASE_ERROR = -10242500
    val HBASE_TIMEOUT = -10242504
    val HBASE_CONN_FAILED = -10242404

    def parseAction(s:String):Int = {
        
        if( s == null ) return ACTION_UNKNOWN

        s.toLowerCase match {
            case "get" => return ACTION_GET 
            case "put" => return ACTION_PUT
            case "delete" => return ACTION_DELETE
            case _ => return ACTION_UNKNOWN
        }
    }

}
   
class HbaseMsg(val serviceId:Int,val msgId:Int,val action:Int,
        val defaultTable:String, val defaultColumnFamily:String,
        val columnNames:ArrayBufferString, val defaultValues:ArrayBufferString, val isDynamics:ArrayBuffer[Boolean]){
    override def toString() = {
        "serviceId=%d,msgId=%s,action=%d,defaultTable=%s,defaultColumnFamily=%s,columnNames=%s,defaultValues=%s,isDynamics=%s".format(
            serviceId,msgId,action,defaultTable,defaultColumnFamily,
            columnNames.mkString(","),defaultValues.mkString(","),isDynamics.mkString(","))
    }
}

class HbaseClient(val serviceIds:String, val cfgNode: Node, val router:Router, val hbaseActor:HbaseActor) 
    extends Logging with Dumpable with SelfCheckLike {

    val msgMap = HashMap[String,HbaseMsg]()
    var hbaseCfg : Configuration = _
    var hconn : HConnection = _
    val lock = new ReentrantLock(false)

    init

    def init() {

        parseMsgCfg()

        val serverAddr = (cfgNode \ "ZooKeeperServerAddr").text
        val clientPort = (cfgNode \ "ZooKeeperServerPort").text
        val connectTimeout = (cfgNode \ "ConnectTimeout").text
        val timeout = (cfgNode \ "Timeout").text

        val config = new Configuration()
        config.set("hbase.zookeeper.quorum", serverAddr)
        config.set("hbase.zookeeper.property.clientPort", clientPort)
        if( connectTimeout != "" )
            config.set("ipc.socket.timeout", connectTimeout)
        if( timeout != "" )
            config.set("hbase.rpc.timeout", timeout)

        hbaseCfg = HBaseConfiguration.create(config)
        hconn = HConnectionManager.createConnection(hbaseCfg)

        log.info("hbase client started")
    }

    def close() {
        if( hconn != null ) {
            try {
                hconn.close()
            } catch {
                case e:Throwable =>
                    log.error("hbase close connection error, e="+e.getMessage)
            }
            hconn = null
        }
        log.info("hbase client stopped")
    }

    def getTable(tableName:String):HTableInterface = {
        val table = hconn.getTable(Bytes.toBytes(tableName))
        table    
    }

    def closeTable(table:HTableInterface) {

        if( table == null ) return

        try {
            table.close()
        } catch {
            case e:Throwable =>
                log.error("hbase close table error, e="+e.getMessage)
        }
    }

    def parseMsgCfg() {

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for( serviceId <- serviceIdArray ) {
            val codec = router.codecs.findTlvCodec(serviceId)

            if( codec == null ) {
                throw new RuntimeException("serviceId not found, serviceId="+serviceId)
            }

            for( (msgId,map) <- codec.msgAttributes  ) {

                val action = parseAction( map.getOrElse(ACTION,null) )
                if(action == ACTION_UNKNOWN ) {
                    throw new RuntimeException(ACTION+" not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }
                
                val reqFields = codec.msgKeyToTypeMapForReq.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList
                val resFields = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList

                if( !reqFields.contains(TABLE_NAME) ) {
                    throw new RuntimeException(TABLE_NAME+" not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }
                if( !reqFields.contains(ROW_KEY) ) {
                    throw new RuntimeException(ROW_KEY+" not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }

                var defaultTable = map.getOrElse("req-"+TABLE_NAME+"-default",null)
                var defaultColumnFamily = map.getOrElse("req-"+COLUMN_FAMILY+"-default",null)

                val columnNames = ArrayBufferString()
                val defaultValues = ArrayBufferString()
                val isDynamics = ArrayBuffer[Boolean]()

                if( action == ACTION_PUT ) {
                    if( !reqFields.contains(COLUMN_FAMILY) ) {
                        throw new RuntimeException(COLUMN_FAMILY+" not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                    }
                    for(f<-reqFields if f != TABLE_NAME if f != COLUMN_FAMILY if f != ROW_KEY ) {
                      columnNames += f
                      val defaultValue = map.getOrElse("req-"+f+"-default",null)
                      defaultValues += defaultValue
                      val isDynamic = map.getOrElse("req-"+f+"-isDynamic","0").toLowerCase
                      val b = ( isDynamic == "1" || isDynamic == "t" || isDynamic == "y" || isDynamic == "true" || isDynamic == "yes" )
                      isDynamics += b
                    }
                    if( columnNames.size == 0 ) {
                        throw new RuntimeException("value not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                    }
                }
                if( action == ACTION_GET ) {
                    if( !reqFields.contains(COLUMN_FAMILY) ) {
                        throw new RuntimeException(COLUMN_FAMILY+" not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                    }
                    for(f<-resFields) {
                      val defaultValue = map.getOrElse("res-"+f+"-default",null)
                      columnNames += f
                      defaultValues += defaultValue
                    }
                    if( columnNames.size == 0 ) {
                        throw new RuntimeException("value not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                    }
                }
                val msg =  new HbaseMsg(serviceId,msgId,action,
                        defaultTable,defaultColumnFamily,
                        columnNames, defaultValues,isDynamics)
                msgMap.put( key(serviceId,msgId),msg)

            }

        }
    }

    def key(serviceId:Int,msgId:Int):String = serviceId + ":" + msgId

    def send(req:Request):Unit = {

        val msg = msgMap.getOrElse(key(req.serviceId,req.msgId),null)
        if( msg == null ) {
          reply(req,HBASE_ERROR)
          return
        }

        val tableName = req.s(TABLE_NAME,msg.defaultTable)
        if( tableName == null ) {
          reply(req,HBASE_ERROR)
          return
        }

        val rowKey = req.s(ROW_KEY)
        if( rowKey == null ) {
          reply(req,HBASE_ERROR)
          return
        }
        val columnFamily = req.s(COLUMN_FAMILY,msg.defaultColumnFamily)

        msg.action match {
            case ACTION_GET =>
                if( columnFamily == null ) {
                  reply(req,HBASE_ERROR)
                  return
                }
                get(req,tableName,rowKey,columnFamily,msg.columnNames,msg.defaultValues)
            case ACTION_PUT =>
                if( columnFamily == null ) {
                  reply(req,HBASE_ERROR)
                  return
                }
                put(req,tableName,rowKey,columnFamily,msg.columnNames,msg.defaultValues,msg.isDynamics)
            case ACTION_DELETE =>
                delete(req,tableName,rowKey)
            case _ =>
                reply(req,HBASE_ERROR)
        }
    }

    def genColumnNames(defaultColumnFamily:String,columnNames:ArrayBufferString):Tuple2[ArrayBufferString,ArrayBufferString] = {

        val cfs = ArrayBufferString()
        val fns = ArrayBufferString()

        var i = 0
        while(i< columnNames.size) {
            var f = columnNames(i)
            if( f.indexOf(".") >= 0 ) {
                val ss = f.split("\\.")
                cfs += ss(0)
                fns += ss(1)
            } else {
                cfs += defaultColumnFamily
                fns += f
            }
            i+=1    
        }

        (cfs,fns)
    }

    def get(req:Request,tableName:String,rowKey:String,defaultColumnFamily:String,columnNames:ArrayBufferString,defaultValues:ArrayBufferString) {

        val (cfs,fns) = genColumnNames(defaultColumnFamily,columnNames)

        var table:HTableInterface = null
        var result:Result = null
        try{

            val g = new Get(Bytes.toBytes(rowKey))
            var i = 0
            while(i< cfs.size) {
                g.addColumn(Bytes.toBytes(cfs(i)),Bytes.toBytes(fns(i)))
                i += 1    
            }

            table = getTable(tableName)
            result = table.get(g)
        } catch {
            case e :Throwable =>
                log.error("hbase get error, e="+e.getMessage+",req="+req.toString)
                throw e
        } finally {
            closeTable(table)
        }

        val map = new HashMapStringAny()
        var i = 0
        while(i< cfs.size) {
            var value =  Bytes.toString(result.getValue(Bytes.toBytes(cfs(i)),Bytes.toBytes(fns(i))))
            if( value == null ) value = defaultValues(i)
            map.put(columnNames(i),value)
            i += 1    
        }

        reply(req,0,map)
    }

    def put(req:Request,tableName:String,rowKey:String,defaultColumnFamily:String,columnNames:ArrayBufferString,defaultValues:ArrayBufferString,isDynamics:ArrayBuffer[Boolean]) {
        val (cfs,fns) = genColumnNames(defaultColumnFamily,columnNames)

        var table:HTableInterface = null
        try{

            val p = new Put(Bytes.toBytes(rowKey))
            var i = 0
            while(i< cfs.size) {
                var value = req.s(columnNames(i),defaultValues(i))
                if( value == null && isDynamics(i) ) {
                    // ignore dynamic fields
                } else {
                    p.add(Bytes.toBytes(cfs(i)),Bytes.toBytes(fns(i)),if( value != null ) Bytes.toBytes(value) else null ) 
                }
                i += 1    
            }
            
            table = getTable(tableName)
            table.put(p)
        } catch {
            case e :Throwable =>
                log.error("hbase put error, e="+e.getMessage+",req="+req.toString)
                throw e
        } finally {
            closeTable(table)
        }

        reply(req,0)
    }

    def delete(req:Request,tableName:String,rowKey:String) {

        var table:HTableInterface = null
        try{

            val d = new Delete(Bytes.toBytes(rowKey))

            table = getTable(tableName)
            table.delete(d)
        } catch {
            case e :Throwable =>
                log.error("hbase delete error, e="+e.getMessage+",req="+req.toString)
                throw e
        } finally {
            closeTable(table)
        }

        reply(req,0)
    }

    def reply(req:Request, code:Int) :Unit ={
        reply(req,code,new HashMapStringAny())
    }

    def reply(req:Request, code:Int,params:HashMapStringAny):Unit = {
        val res = new Response(code,params,req)
        hbaseActor.reply(new RequestResponseInfo(req,res))
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = new ArrayBuffer[SelfCheckResult]()
        buff
    }

    def dump() {
    }

}



