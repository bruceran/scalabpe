package scalabpe.plugin.hadoop

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
import org.apache.hadoop.hbase.filter._

import scalabpe.core._

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
    val ACTION_SCAN = 4
    val ACTION_GETDYNAMIC = 5
    val ACTION_PUTDYNAMIC = 6
    val ACTION_SCANDYNAMIC = 7

    val ROW_KEY = "rowKey"
    val TABLE_NAME = "tableName"
    val ROW_COUNT = "rowCount"

    val EMPTY_STRINGMAP = new HashMapStringString()

    val HBASE_ERROR = -10242500
    val HBASE_TIMEOUT = -10242504
    val HBASE_CONN_FAILED = -10242404

    def parseAction(s:String):Int = {
        
        if( s == null || s == "" ) return ACTION_UNKNOWN

        s.toLowerCase match {
            case "get" => return ACTION_GET 
            case "getdynamic" => return ACTION_GETDYNAMIC
            case "put" => return ACTION_PUT
            case "putdynamic" => return ACTION_PUTDYNAMIC
            case "delete" => return ACTION_DELETE
            case "scan" => return ACTION_SCAN
            case "scandynamic" => return ACTION_SCANDYNAMIC
            case _ => return ACTION_UNKNOWN
        }
    }

}
   
class ColumnInfo(val fn:String,val cf:String,val cn:String) {
    var isDynamic: Boolean = false
    var compare: String = "="
}

class HbaseMsg(val serviceId:Int,val msgId:Int,val action:Int, val tableName:String,val columnFamily:String, 
    val reqInfos:ArrayBuffer[ColumnInfo],val resInfos:ArrayBuffer[ColumnInfo] ) {
    override def toString() = {
        "serviceId=%d,msgId=%s,action=%d,tableName=%s".format( serviceId,msgId,action,tableName)
    }
}

class HbaseClient(val serviceIds:String, val cfgNode: Node, val router:Router, val hbaseActor:HbaseActor) 
    extends Logging with Dumpable with SelfCheckLike {

    val msgMap = HashMap[String,HbaseMsg]()
    var hbaseCfg : Configuration = _
    var hconn : Connection = _
    val lock = new ReentrantLock(false)

    init

    def init() {

        parseMsgCfg()

        val serverAddr = (cfgNode \ "ZooKeeperServerAddr").text
        val clientPort = (cfgNode \ "ZooKeeperServerPort").text

        val config = new Configuration()
        config.set("hbase.zookeeper.quorum", serverAddr)
        config.set("hbase.zookeeper.property.clientPort", clientPort)

        /*
        val timeout = (cfgNode \ "Timeout").text

        if( timeout != "" ) {
            config.set("hbase.rpc.timeout", timeout) // 貌似没有用
            config.set("hbase.client.operation.timeout", timeout) // 貌似没有用
        }
        */

        // regionserver连不上改成重试1次就返回结果
        config.set("hbase.client.retries.number", "1") // 每次重试时间可参考 hbase.client.pause 参数说明 

        hbaseCfg = HBaseConfiguration.create(config)

        log.info("HbaseActor creating connection {}",serviceIds)
        // 在这一步，zookeeper连不上会一直重试，导致程序无法启动
        hconn = ConnectionFactory.createConnection(hbaseCfg)
        log.info("HbaseActor connection connected {}",serviceIds)

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

    def getTable(tableName:String):Table = {
        val table = hconn.getTable(TableName.valueOf(tableName))
        table    
    }

    def closeTable(table:Table) {

        if( table == null ) return

        try {
            table.close()
        } catch {
            case e:Throwable =>
                log.error("hbase close table error, e="+e.getMessage)
        }
    }

    def closeResults(results:ResultScanner) {

        if( results == null ) return

        try {
            results.close()
        } catch {
            case e:Throwable =>
                log.error("hbase close results error, e="+e.getMessage)
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

                val action = parseAction( map.getOrElse("action","") )
                if(action == ACTION_UNKNOWN ) {
                    throw new RuntimeException("action not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }
                val tableName = map.getOrElse("tableName","") 
                if( tableName == "" ) {
                    throw new RuntimeException("tableName not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }
                val columnFamily = map.getOrElse("columnFamily","") 
                if( action != ACTION_DELETE && columnFamily == "" ) {
                    throw new RuntimeException("columnFamily not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }

                val reqFields = codec.msgKeyToTypeMapForReq.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList
                val resFields = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList

                if( action !=  ACTION_SCAN && action !=  ACTION_SCANDYNAMIC && !reqFields.contains(ROW_KEY) ) {
                    throw new RuntimeException(ROW_KEY+" not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                }

                val reqInfos = new ArrayBuffer[ColumnInfo]()
                val resInfos = new ArrayBuffer[ColumnInfo]()

                action match {
                    case ACTION_PUT =>
                        for(f<-reqFields if f != ROW_KEY && f != TABLE_NAME && f != "values" ) {
                          val (cf,cn) = genColumnInfo(columnFamily,f)
                          val isDynamic = map.getOrElse("req-"+f+"-isDynamic","0").toLowerCase
                          val b = ( isDynamic == "1" || isDynamic == "t" || isDynamic == "y" || isDynamic == "true" || isDynamic == "yes" )
                          val ci = new ColumnInfo(f,cf,cn)
                          ci.isDynamic = b
                          reqInfos += ci
                        }
                        if( reqInfos.size == 0 ) {
                            throw new RuntimeException("value not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                        }
                    case ACTION_GET =>
                        for(f<-resFields if f != ROW_COUNT ) {
                          val (cf,cn) = genColumnInfo(columnFamily,f)
                          val ci = new ColumnInfo(f,cf,cn)
                          resInfos += ci
                        }
                        if( resInfos.size == 0 ) {
                            throw new RuntimeException("value not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                        }
                    case ACTION_SCAN =>
                        for(f<-resFields if f != ROW_KEY && f != ROW_COUNT ) {
                          val (cf,cn) = genColumnInfo(columnFamily,f)
                          val ci = new ColumnInfo(f,cf,cn)
                          resInfos += ci
                        }
                        if( resInfos.size == 0 ) {
                            throw new RuntimeException("value not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                        }
                    case _ =>
                }
                val msg =  new HbaseMsg(serviceId,msgId,action,tableName,columnFamily,reqInfos,resInfos)
                msgMap.put( key(serviceId,msgId),msg)

            }

        }
    }

    def genColumnInfo(defaultColumnFamily:String,fieldName:String):Tuple2[String,String] = {
        val p = fieldName.indexOf(".")
        if( p >= 0 ) {
            val cf = fieldName.substring(0,p)
            val cn = fieldName.substring(p+1)
            return ( cf,cn )
        } else {
            return ( defaultColumnFamily,fieldName)
        }
    }

    def toFilter(columnFamily:String,resInfos:ArrayBuffer[ColumnInfo],filter:String):Filter = {
        val expr = FilterExprParser.parse(filter)
        if( expr != null )
            return expr.eval(columnFamily,resInfos)
        null
    }

    def key(serviceId:Int,msgId:Int):String = serviceId + ":" + msgId

    def send(req:Request):Unit = {

        val msg = msgMap.getOrElse(key(req.serviceId,req.msgId),null)
        if( msg == null ) {
          reply(req,HBASE_ERROR)
          return
        }

        msg.action match {
            case ACTION_GET =>
                get(msg,req)
            case ACTION_GETDYNAMIC =>
                getDynamic(msg,req)
            case ACTION_PUT =>
                put(msg,req)
            case ACTION_PUTDYNAMIC =>
                putDynamic(msg,req)
            case ACTION_DELETE =>
                delete(msg,req)
            case ACTION_SCAN =>
                scan(msg,req)
            case ACTION_SCANDYNAMIC =>
                scanDynamic(msg,req)
            case _ =>
                reply(req,HBASE_ERROR)
        }
    }

    def get(msg:HbaseMsg,req:Request) {
        val rowKey = req.s(ROW_KEY,"")
        if( rowKey == "" ) {
          reply(req,HBASE_ERROR)
          return
        }
        
        var table:Table = null
        var result:Result = null
        try{

            val g = new Get(Bytes.toBytes(rowKey))
            var i = 0
            while(i< msg.resInfos.size) {
                val ci = msg.resInfos(i)
                g.addColumn(Bytes.toBytes(ci.cf),Bytes.toBytes(ci.cn))
                i += 1    
            }

            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
            table = getTable(tableName)
            result = table.get(g)
        } catch {
            case e :Throwable =>
                log.error("hbase get error, e="+e.getMessage+",req="+req.toString)
                throw e
        } finally {
            closeTable(table)
        }

        //println("result.isEmpty="+result.isEmpty())

        val map = new HashMapStringAny()
        if( result.isEmpty ) {
            map.put("rowCount",0)
        } else {
            map.put("rowCount",1)
            var i = 0
            while(i< msg.resInfos.size) {
                val ci = msg.resInfos(i)
                var value =  Bytes.toString(result.getValue(Bytes.toBytes(ci.cf),Bytes.toBytes(ci.cn)))
                map.put(ci.fn,value)
                i += 1    
            }
        }

        reply(req,0,map)
    }

    def getDynamic(msg:HbaseMsg,req:Request) {
        val rowKey = req.s(ROW_KEY,"")
        if( rowKey == "" ) {
          reply(req,HBASE_ERROR)
          return
        }
        
        val fields = req.s("fields")
        if( fields == null || fields == "" ) {
          reply(req,HBASE_ERROR)
          return
        }
        var ss = fields.split(",")

        var table:Table = null
        var result:Result = null
        try{

            val g = new Get(Bytes.toBytes(rowKey))
            var i = 0
            while(i< ss.size) {
                val field = ss(i)
                val (cf,cn) = genColumnInfo(msg.columnFamily,field)
                g.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn))
                i += 1    
            }

            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
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
        if( result.isEmpty ) {
            map.put("rowCount",0)
        } else {
            map.put("rowCount",1)
            val rm = HashMapStringAny()
            var i = 0
            while(i< ss.size) {
                val field = ss(i)
                val (cf,cn) = genColumnInfo(msg.columnFamily,field)
                var value =  Bytes.toString(result.getValue(Bytes.toBytes(cf),Bytes.toBytes(cn)))
                rm.put(field,value)
                i += 1    
            }
            map.put("values",JsonCodec.mkString(rm))
        }

        reply(req,0,map)
    }

    def put(msg:HbaseMsg,req:Request) {
        val rowKey = req.s(ROW_KEY,"")
        if( rowKey == "" ) {
          reply(req,HBASE_ERROR)
          return
        }

        val fieldValues = req.s("values","")

        var table:Table = null
        try{

            val p = new Put(Bytes.toBytes(rowKey))
            var i = 0
            while(i< msg.reqInfos.size) {
                val ci = msg.reqInfos(i)
                var value = req.s(ci.fn)
                if( value == null && ci.isDynamic ) {
                    // ignore dynamic fields
                } else {
                    p.addColumn(Bytes.toBytes(ci.cf),Bytes.toBytes(ci.cn),if( value != null ) Bytes.toBytes(value) else null ) 
                }
                i += 1    
            }
            
            if( fieldValues != "" ) {
                val m = JsonCodec.parseObject(fieldValues)
                if( m != null ) {
                    for( (field,v) <- m ) {
                        val value = TypeSafe.anyToString(v)
                        val (cf,cn) = genColumnInfo(msg.columnFamily,field)
                        p.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),if( value != null ) Bytes.toBytes(value) else null ) 
                    }
                }
            }

            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
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

    def putDynamic(msg:HbaseMsg,req:Request) {
        val rowKey = req.s(ROW_KEY,"")
        if( rowKey == "" ) {
          reply(req,HBASE_ERROR)
          return
        }

        val fieldValues = req.s("values")
        if( fieldValues == null || fieldValues == "" ) {
          reply(req,HBASE_ERROR)
          return
        }

        var table:Table = null
        try{

            val p = new Put(Bytes.toBytes(rowKey))
            val m = JsonCodec.parseObject(fieldValues)
            for( (field,v) <- m ) {
                val value = TypeSafe.anyToString(v)
                val (cf,cn) = genColumnInfo(msg.columnFamily,field)
                p.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),if( value != null ) Bytes.toBytes(value) else null ) 
            }
            
            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
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

    def delete(msg:HbaseMsg,req:Request) {
        val rowKey = req.s(ROW_KEY,"")
        if( rowKey == "" ) {
          reply(req,HBASE_ERROR)
          return
        }

        var table:Table = null
        try{

            val d = new Delete(Bytes.toBytes(rowKey))
            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
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

    def scan(msg:HbaseMsg,req:Request) {
        val startRow = req.s("startRow","")
        val stopRow = req.s("stopRow","")
        val reverse = req.i("reverse")

        val maxResultSize = req.i("maxResultSize",10)
        val caching = req.i("caching",10)

        var table:Table = null
        var results:ResultScanner = null
        val map = new HashMapStringAny()
        
        try{

            val s = new Scan()
            if( startRow != "" ) 
                s.setStartRow(Bytes.toBytes(startRow))
            if( stopRow != "" ) 
                s.setStopRow(Bytes.toBytes(stopRow))
            s.setMaxResultSize(maxResultSize)
            s.setCaching(caching)
            if( reverse == 1 ) s.setReversed(true)
            var i = 0
            while(i< msg.resInfos.size) {
                val ci = msg.resInfos(i)
                s.addColumn(Bytes.toBytes(ci.cf),Bytes.toBytes(ci.cn))
                i += 1    
            }

            val filter = req.s("filter","")
            if( filter != "" ) {
                val f = toFilter(msg.columnFamily,msg.resInfos,filter)
                if( f != null )
                    s.setFilter(f)
            }

            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
            table = getTable(tableName)
            results = table.getScanner(s)

            i = 0
            while(i< msg.resInfos.size) {
                val ci = msg.resInfos(i)
                map.put(ci.fn,ArrayBufferString())
                i += 1    
            }
            val rowKeys = ArrayBufferString()
            map.put(ROW_KEY,rowKeys)

            var result = results.next()
            var cnt = 0
            while( result != null && cnt < maxResultSize ) {
                var i = 0
                while(i< msg.resInfos.size) {
                    val ci = msg.resInfos(i)
                    var value =  Bytes.toString(result.getValue(Bytes.toBytes(ci.cf),Bytes.toBytes(ci.cn)))
                    val arr = map.ls(ci.fn)
                    arr += value
                    i += 1    
                }
                val rowKey = Bytes.toString(result.getRow())
                rowKeys += rowKey

                cnt += 1
                if( cnt < maxResultSize )
                    result = results.next()
            }
            map.put(ROW_COUNT,cnt)
            
        } catch {
            case e :Throwable =>
                log.error("hbase get error, e="+e.getMessage+",req="+req.toString)
                throw e
        } finally {
            closeResults(results)
            closeTable(table)
        }
        reply(req,0,map)
    }

    def scanDynamic(msg:HbaseMsg,req:Request) {
        val startRow = req.s("startRow","")
        val stopRow = req.s("stopRow","")
        val reverse = req.i("reverse")

        val maxResultSize = req.i("maxResultSize",10)
        val caching = req.i("caching",10)

        val fields = req.s("fields")
        if( fields == null || fields == "" ) {
          reply(req,HBASE_ERROR)
          return
        }
        var ss = fields.split(",")

        var table:Table = null
        var results:ResultScanner = null
        val map = new HashMapStringAny()
        
        try{

            val s = new Scan()
            if( startRow != "" ) 
                s.setStartRow(Bytes.toBytes(startRow))
            if( stopRow != "" ) 
                s.setStopRow(Bytes.toBytes(stopRow))
            s.setMaxResultSize(maxResultSize)
            s.setCaching(caching)
            if( reverse == 1 ) s.setReversed(true)
            var i = 0
            while(i< ss.size) {
                val field = ss(i)
                val (cf,cn) = genColumnInfo(msg.columnFamily,field)
                s.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn))
                i += 1    
            }

            val filter = req.s("filter","")
            if( filter != "" ) {
                val f = toFilter(msg.columnFamily,msg.resInfos,filter)
                if( f != null )
                    s.setFilter(f)
            }

            val tableName = if ( req.s("tableName","") != "" ) req.s("tableName")  else msg.tableName
            table = getTable(tableName)
            results = table.getScanner(s)

            val fieldValuesArray = ArrayBufferString()

            var result = results.next()
            var cnt = 0
            while( result != null && cnt < maxResultSize ) {
                val kvs = HashMapStringAny()
                var i = 0
                while(i< ss.size) {
                    val field = ss(i)
                    val (cf,cn) = genColumnInfo(msg.columnFamily,field)
                    var value =  Bytes.toString(result.getValue(Bytes.toBytes(cf),Bytes.toBytes(cn)))
                    kvs.put(field,value)
                    i += 1    
                }
                val rowKey = Bytes.toString(result.getRow())
                kvs.put("rowKey",rowKey)
                fieldValuesArray += JsonCodec.mkString(kvs)

                cnt += 1
                if( cnt < maxResultSize )
                    result = results.next()
            }
            map.put(ROW_COUNT,cnt)
            map.put("rows",fieldValuesArray)
            
        } catch {
            case e :Throwable =>
                log.error("hbase get error, e="+e.getMessage+",req="+req.toString)
                throw e
        } finally {
            closeResults(results)
            closeTable(table)
        }
        reply(req,0,map)
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



