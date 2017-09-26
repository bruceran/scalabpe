package scalabpe.plugin.neo4j

import java.util.concurrent._
import scala.collection.mutable.{HashMap,ArrayBuffer,HashSet}
import scala.xml._

import org.neo4j.driver.v1._

import scalabpe.core._

class DbConfig ( val connNum: Int, val connStr: String) {

    override def toString() = {
        ("connNum=%d,connStr=%s").format(connNum,connStr)
    }

}

class DbActor(override val router:Router,override val cfgNode: Node) extends BaseDbActor(router,cfgNode,DbLike.MODE_ASYNC) {

    init

    override def init() {
        super.init
    }

    override def receive(v:Any) :Unit = {

        v match {

            case req: Request =>

                // log.info("request received, req={}",req.toString)
                try {

                    pool.execute( new Runnable() {
                        def run() {

                            val now = System.currentTimeMillis
                            if( req.receivedTime + req.expireTimeout < now ) {
                                db.reply(req,ResultCodes.SERVICE_BUSY)
                                log.error("db is busy, req expired, req={}",req)
                                return
                            }

                            try {
                                db.process(req)
                            } catch {
                                case e:Exception =>
                                    val errorCode = ErrorCodeUtils.parseErrorCode(e)
                                    db.reply(req,errorCode)
                                    log.error("db exception req={}",req,e)
                            }

                        }
                    } )

                } catch {
                    case e: RejectedExecutionException =>
                        db.reply(req,ResultCodes.SERVICE_FULL)
                        log.error("db queue is full, serviceIds={}",serviceIds)
                }

            case _ =>

                log.error("unknown msg")

        }
    }


    def reply(reqResInfo: RequestResponseInfo):Unit = {
        router.reply(reqResInfo)
    }

}

class SyncedDbActor(override val router:Router,override val cfgNode: Node) extends BaseDbActor(router,cfgNode,DbLike.MODE_SYNC) with SyncedActor {

    val retmap = new ConcurrentHashMap[String,Response]()

    init

    override def init() {
        super.init
    }

    override def receive(v:Any) :Unit = {

        v match {

            case req: Request =>

                val now = System.currentTimeMillis
                if( req.receivedTime + req.expireTimeout < now ) {
                    db.reply(req,ResultCodes.SERVICE_BUSY)
                    log.error("db is busy, req expired, req={}",req)
                    return
                }

                try {
                    db.process(req) // use the caller's thread to do db operation
                } catch {
                    case e:Exception =>
                        val errorCode = ErrorCodeUtils.parseErrorCode(e)
                        db.reply(req,errorCode)
                        log.error("db exception req={}",req,e)
                }

            case _ =>

                log.error("unknown msg")
        }
    }

    def reply(reqResInfo: RequestResponseInfo):Unit = {

        val (newbody,ec) = router.encodeResponse(reqResInfo.res.serviceId,reqResInfo.res.msgId,reqResInfo.res.code,reqResInfo.res.body)
        var errorCode = reqResInfo.res.code
        if( errorCode == 0 && ec != 0 ) {
            errorCode = ec
        }

        val res = new Response(errorCode,newbody,reqResInfo.req)
        retmap.put(reqResInfo.req.requestId,res)
    }

    def get(requestId:String): Response = {
        retmap.remove(requestId)
    }
}

abstract class BaseDbActor(val router:Router,val cfgNode: Node,val dbMode:Int ) extends Actor with Logging  with Closable with SelfCheckLike  with Dumpable {

    var serviceIds: String = _
    var db: DbClient = _

    val queueSize = 20000
    var threadFactory : ThreadFactory = _
    var pool : ThreadPoolExecutor = _

    def dump() {

        log.info("--- serviceIds="+serviceIds)

        if( pool != null ) {
            val buff = new StringBuilder

            buff.append("pool.size=").append(pool.getPoolSize).append(",")
            buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

            log.info(buff.toString)
        }

        db.dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        var connStr = ( cfgNode \ "Connection" ).text
        var conns = 8
        var threadNum = 8
        
        var s = ( cfgNode \ "@conns" ).text
        if( s != "" ) conns = s.toInt

        val cfg = new DbConfig(conns,connStr)

        db = new DbClient(serviceIds,cfg,router,this,dbMode)

        s = (router.cfgXml \ "LongTimeSql").text
        if( s != "" )
            db.longTimeSql = s.toInt

        if( dbMode == DbLike.MODE_ASYNC ) {

            s = (cfgNode \ "@threadNum").text
            if( s != "" ) threadNum = s.toInt

            val firstServiceId = serviceIds.split(",")(0)
            threadFactory = new NamedThreadFactory("db"+firstServiceId)
            pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize),threadFactory)
            pool.prestartAllCoreThreads()

        }

        log.info(getClass.getName+" started {}",serviceIds)
    }

    def close() {

        if( pool != null ) {

            val t1 = System.currentTimeMillis

            pool.shutdown()

            pool.awaitTermination(5,TimeUnit.SECONDS)

            val t2 = System.currentTimeMillis
            if( t2 - t1 > 100 )
                log.warn(getClass.getName+" long time to shutdown pool, ts={}",t2-t1)

            pool = null
        }

        db.close()
        log.info(getClass.getName+" stopped {}",serviceIds)
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = db.selfcheck()
        buff
    }

    def reply(reqResInfo: RequestResponseInfo):Unit
}

object MsgDefine{

    val SQLTYPE_SELECT = 1
    val SQLTYPE_UPDATE = 2
    val SQLTYPE_MULTIUPDATE = 3
    val SQLTYPE_BATCHUPDATE = 4
    val SQLTYPE_TRANSACTION = 5
    val SQLTYPE_UNKNOWN = 6

    val REPLACE_TYPE_UNKNOWN = 0
    val REPLACE_TYPE_PREPARE = 1
    val REPLACE_TYPE_OVERWRITE = 2

    val matchReturnReg = """^match .* return .*$""".r
    val createReturnReg = """^create .* return .*$""".r
    val mergeReturnReg = """^merge .* return .*$""".r
    val matchReg = """^match .*$""".r
    val createReg = """^create .*$""".r
    val mergeReg = """^merge .*$""".r

    def toSqlType(sql:String):Int = {

        val trimedsql = sql.trim()

        val prefix = trimedsql.toLowerCase
        prefix match {
            case matchReturnReg() => MsgDefine.SQLTYPE_SELECT
            case createReturnReg() => MsgDefine.SQLTYPE_SELECT
            case mergeReturnReg() => MsgDefine.SQLTYPE_SELECT
            case matchReg() => MsgDefine.SQLTYPE_UPDATE
            case createReg() => MsgDefine.SQLTYPE_UPDATE
            case mergeReg() => MsgDefine.SQLTYPE_UPDATE
            case _ => MsgDefine.SQLTYPE_UNKNOWN
        }
    }

    val RESULTTYPE_ROWCOUNT = 1
    val RESULTTYPE_ROW = 2
    val RESULTTYPE_COLUMN = 3
    val RESULTTYPE_ALL = 4
    val RESULTTYPE_SQLCODE = 5
    val RESULTTYPE_COLUMNLIST = 6

}

class RequestDefineOriginal(
    val key:String,
    val idx:String, // used to replace :idx
    val defaultValue:String = null,
    val columnType:String = null,
    val replaceType:Int = MsgDefine.REPLACE_TYPE_UNKNOWN 
) {
    override def toString() = {
        "key=%s,idx=%s,defaultValue=%s,columnType=%s,replaceType=%d".format(key,idx,defaultValue,columnType,replaceType)
    }
}

class RequestDefine(
    val key:String,
    val defaultValue:String = null
) {
    override def toString() = {
        "key=%s,defaultValue=%s".format(key,defaultValue)
    }
}

class RequestDefineOverwrite(
    val key:String,
    val idx:String,
    val defaultValue:String = null
) {
    override def toString() = {
        "key=%s,idx=%s,defaultValue=%s".format(key,idx,defaultValue)
    }
}

class MsgSql(
    val sql : String,
    val keys : ArrayBuffer[RequestDefine],
    val paramNames : ArrayBufferString,
    val paramTypes : ArrayBuffer[Int],
    val overwriteBuff : ArrayBuffer[RequestDefineOverwrite]) {

    override def toString() = {
        var keys_str = ""
        if( keys != null ) keys_str = keys.mkString(",")
        var paramTypes_str = ""
        if( paramTypes != null ) paramTypes_str = paramTypes.mkString(",")
        var overwriteBuff_str = ""
        if( overwriteBuff != null ) overwriteBuff_str = overwriteBuff.mkString(",")
        "sql=%s,keys=%s,paramTypes=%s,overwriteMap=%s".format(sql,keys_str,paramTypes_str,overwriteBuff_str)
    }
}

class ResultDefine(
        val key:String,
        val fieldType:Int,
        val row:Int, // used when fieldType == TYPE_COLUMN | TYPE_ROW
        val col:Int, // used when fieldType == TYPE_COLUMN | TYPE_COLMUNLIST
        val rowNames:List[String] ) { // used when fieldType == TYPE_ROW | TYPE_ALL, map to a avenue struct
    def this(key:String,fieldType:Int) {
        this(key,fieldType,0,0,List[String]())
    }
    def this(key:String,fieldType:Int,row:Int,col:Int) {
        this(key,fieldType,row,col,List[String]())
    }
    override def toString() = {
        "key=%s,fieldType=%d,row=%d,col=%d,rowNames=%s".format(key,fieldType,row,col,rowNames.mkString(","))
    }
}

class MsgDefine (
    val sqlType :Int,  
    val sqls : ArrayBuffer[MsgSql],
    val resdefs : ArrayBuffer[ResultDefine] )  {

    override def toString() = {
        "sqlType=%d,sqls=%s,resdefs=%s".format(sqlType,sqls.mkString(","),resdefs.mkString(","))
    }

}

object DbClient {

    val reg = """^[{]([0-9a-zA-Z_]+)[}]$""".r
    val regOverWrite = """^\$([0-9a-zA-Z_]+)$""".r

    val reg1 = """^\$ROWCOUNT$""".r  // $ROWCOUNT
    val reg2 = """^\$result\[([0-9]+)\]\[([0-9]+)\]$""".r  // $result[n][m]
    val reg2list = """^\$result\[\*\]\[([0-9]+)\]$""".r  // $result[*][m]
    val reg3 = """^\$result\[([0-9]+)\]$""".r  // $result[n]
    val reg4 = """^\$result$""".r  // $result
    val reg5 = """^\$SQLCODE$""".r  // $SQLCODE

    // used to parse sql, replace :xxx to ?

    val sqlReg2 = """[{][0-9a-zA-Z_]+[}]""".r
    val sqlReg3 = """^[{]([0-9a-zA-Z_]+)[}]$""".r  // :placeholder

    val sqlReg2Overwrite = """\$[0-9a-zA-Z_]+[ ,)]|\$[0-9a-zA-Z_]+$""".r
    val sqlReg3Overwrite = """^\$([0-9a-zA-Z_]+).*$""".r

    val EMPTY_STRINGMAP = new HashMapStringString()

}

class DbClient(
    val serviceIds: String,
    val dbConfig:DbConfig,
    val router: Router,
    val dbActor: BaseDbActor,
    val dbMode: Int) extends DbLike  with Dumpable {

    import scalabpe.plugin.neo4j.DbClient._

    mode = dbMode

    val msgMap = HashMap[String,MsgDefine]()

    var driver : Driver = null

    val sqlSelectFieldsMap = new HashMap[String,ArrayBuffer[String]]()

    init

    def dump() {
        val buff = new StringBuilder
        log.info(buff.toString)
    }

    def close() {
        closeDriver(driver)
    }

    def isTrue(s:String):Boolean = {
        s == "1"  || s == "t"  || s == "T" || s == "true"  || s == "TRUE" || s == "y"  || s == "Y" || s == "yes" || s == "YES" 
    }

    def init() {

        if( dbConfig == null ) throw new RuntimeException("dbConfig not defined")

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for( serviceId <- serviceIdArray ) {
            val codec = router.codecs.findTlvCodec(serviceId)

            if( codec == null ) {
                throw new RuntimeException("serviceId not found, serviceId="+serviceId)
            }

            if( codec!= null) {

                for( (msgId,map) <- codec.msgAttributes if isTransactionMsgId(msgId) ) {

                    val resFields = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList
                    val resdefs = new ArrayBuffer[ResultDefine]()

                    for(f<-resFields) {

                        var fromValue = map.getOrElse("res-"+f+"-from",null)

                        if(fromValue == null ) {
                            f.toLowerCase  match {
                                case "sqlcode" =>
                                    fromValue = "$SQLCODE"
                                case _ =>
                                    throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,from=%s".format(serviceId,msgId,fromValue))
                            }
                        }
                        if(fromValue != null ) {

                            fromValue match {

                                case reg5() =>
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_SQLCODE)
                                    resdefs += resdef
                                case _ =>
                                    throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,from=%s".format(serviceId,msgId,fromValue))

                            }

                        }
                    }

                    var sqlType = MsgDefine.SQLTYPE_TRANSACTION
                    val msgSqls = new ArrayBuffer[MsgSql]
                    msgSqls += new MsgSql("", null, null, null, null)
                    val msgdef = new MsgDefine(sqlType,msgSqls,resdefs)

                    /*
                    if( log.isDebugEnabled() ) {
                        log.debug("msgdef="+msgdef.toString())
                    }
                     */

                    msgMap.put(serviceId+"-"+msgId,msgdef)
                }

                for( (msgId,map) <- codec.msgAttributes if !isTransactionMsgId(msgId) ) {

                    val sql = map.getOrElse("sql",null)
                    if(sql == null ) {
                        throw new RuntimeException("sql not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))
                    }

                    val sqls = splitSql(sql)

                    if( sqls.size == 0 )
                        throw new RuntimeException("sql not defined for serviceId=%d,msgId=%d".format(serviceId,msgId))

                    var sqlType = MsgDefine.SQLTYPE_UNKNOWN
                    for(sql <- sqls) {
                        sqlType = MsgDefine.toSqlType(sql)
                        if( sqlType == MsgDefine.SQLTYPE_UNKNOWN )
                            throw new RuntimeException("sql is not valid,sql=%s".format(sql))
                        if( sqls.size>1 && sqlType == MsgDefine.SQLTYPE_SELECT )
                            throw new RuntimeException("select is not allowed in multi sqls,sql=%s".format(sql))
                    }

                    if( sqls.size > 1 )
                        sqlType = MsgDefine.SQLTYPE_MULTIUPDATE

                    val reqFields = codec.msgKeyToTypeMapForReq.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList
                    val reqMapOrigBuff = new ArrayBuffer[RequestDefineOriginal]()

                    var hasArrayReqFields = false

                    for(f<-reqFields) {

                        val tlvTypeName = codec.msgKeyToTypeMapForReq.getOrElse(msgId,EMPTY_STRINGMAP).getOrElse(f,null)
                        if( tlvTypeName == null ) {
                            throw new RuntimeException("key not found error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                        }
                        val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName,null)
                        if( tlvType == null ) {
                            throw new RuntimeException("key not found error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                        }
                        if( tlvType.cls != TlvType.CLS_STRING &&
                            tlvType.cls != TlvType.CLS_INT &&
                            tlvType.cls != TlvType.CLS_LONG &&
                            tlvType.cls != TlvType.CLS_DOUBLE &&
                            tlvType.cls != TlvType.CLS_STRINGARRAY &&
                            tlvType.cls != TlvType.CLS_INTARRAY &&
                            tlvType.cls != TlvType.CLS_LONGARRAY &&
                            tlvType.cls != TlvType.CLS_DOUBLEARRAY )
                        throw new RuntimeException("type not supported in request parameter, serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))

                        if( tlvType.cls == TlvType.CLS_STRINGARRAY ||
                            tlvType.cls == TlvType.CLS_INTARRAY ||
                            tlvType.cls == TlvType.CLS_LONGARRAY ||
                            tlvType.cls == TlvType.CLS_DOUBLEARRAY )
                            hasArrayReqFields = true

                        val defaultValue = map.getOrElse("req-"+f+"-default",null)
                        var columnType = map.getOrElse("req-"+f+"-columnType","string")
                        if( columnType == "string" ) {
                            if( tlvType.cls == TlvType.CLS_INT || tlvType.cls == TlvType.CLS_INTARRAY || 
                                tlvType.cls == TlvType.CLS_LONG || tlvType.cls == TlvType.CLS_LONGARRAY ||
                                tlvType.cls == TlvType.CLS_DOUBLE || tlvType.cls == TlvType.CLS_DOUBLEARRAY 
                                )
                                columnType = "number"
                        }
                        val toValue = map.getOrElse("req-"+f+"-to",null)
                        if(toValue != null ) {

                            toValue match {

                                case reg(s) =>
                                    val reqdef = new RequestDefineOriginal(f,s,defaultValue,columnType,MsgDefine.REPLACE_TYPE_PREPARE)
                                    reqMapOrigBuff += reqdef
                                case regOverWrite(s) =>
                                    val reqdef = new RequestDefineOriginal(f,s,defaultValue,columnType,MsgDefine.REPLACE_TYPE_OVERWRITE)
                                    reqMapOrigBuff += reqdef
                                case _ =>
                                    throw new RuntimeException("to defination error for serviceId=%d,msgId=%d,to=%s".format(serviceId,msgId,toValue))
                            }
                        } else {
                            val reqdef = new RequestDefineOriginal(f,f,defaultValue,columnType)
                            reqMapOrigBuff += reqdef
                        }
                    }

                    val resFields = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).keys.toList

                    val resdefs = new ArrayBuffer[ResultDefine]()

                    for(f<-resFields) {

                        var fromValue = map.getOrElse("res-"+f+"-from",null)
                        var ignoreCheck = map.getOrElse("res-"+f+"-ignoreCheck","0") == "1"

                        if(fromValue == null ) {

                            if( ignoreCheck ) {
                                throw new RuntimeException("from must be defined with ignoreCheck for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                            }

                            f.toLowerCase  match {

                                case "rowcount" =>
                                    fromValue = "$ROWCOUNT"

                                case "row_count" =>
                                    fromValue = "$ROWCOUNT"

                                case "sqlcode" =>
                                    fromValue = "$SQLCODE"

                                case "sql_code" =>
                                    fromValue = "$SQLCODE"

                                case _ =>

                                    val tlvTypeName = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).getOrElse(f,null)
                                    if( tlvTypeName == null ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                                    }
                                    val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName,null)
                                    if( tlvType == null ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                                    }

                                    tlvType.cls match {

                                        case TlvType.CLS_STRING =>

                                            val col = findIndexInSelect(sqls(0),sqlType,f)
                                            if( col == -1 )
                                                throw new RuntimeException("from not defined for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))

                                            fromValue = "$result[0][%d]".format(col)

                                        case TlvType.CLS_INT | TlvType.CLS_LONG | TlvType.CLS_DOUBLE =>

                                            val col = findIndexInSelect(sqls(0),sqlType,f)
                                            if( col == -1 )
                                                throw new RuntimeException("from not defined for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))

                                            fromValue = "$result[0][%d]".format(col)

                                        case TlvType.CLS_STRUCT | TlvType.CLS_OBJECT =>

                                            fromValue = "$result[0]"

                                        case TlvType.CLS_STRINGARRAY =>

                                            val col = findIndexInSelectForList(sqls(0),sqlType,f)
                                            if( col == -1 )
                                                throw new RuntimeException("from not defined for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))

                                            fromValue = "$result[*][%d]".format(col)

                                        case TlvType.CLS_INTARRAY | TlvType.CLS_LONGARRAY | TlvType.CLS_DOUBLEARRAY =>

                                            val col = findIndexInSelectForList(sqls(0),sqlType,f)
                                            if( col == -1 )
                                                throw new RuntimeException("from not defined for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))

                                            fromValue = "$result[*][%d]".format(col)

                                        case TlvType.CLS_STRUCTARRAY | TlvType.CLS_OBJECTARRAY =>
                                            fromValue = "$result"
                                    }

                            }

                        }

                        if(fromValue != null ) {

                            fromValue match {

                                case reg1() =>
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_ROWCOUNT)
                                    resdefs += resdef
                                case reg5() =>
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_SQLCODE)
                                    resdefs += resdef
                                case reg2(row,col) =>
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_COLUMN,row.toInt,col.toInt)
                                    resdefs += resdef
                                case reg2list(col) =>
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_COLUMNLIST,0,col.toInt)
                                    resdefs += resdef
                                case reg3(row) =>
                                    val tlvTypeName = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).getOrElse(f,null)
                                    if( tlvTypeName == null ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                                    }
                                    val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName,null)
                                    if( tlvType == null ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                                    }
                                    if( tlvType.cls != TlvType.CLS_STRUCT &&  tlvType.cls != TlvType.CLS_OBJECT ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s, the tlv type must struct".format(serviceId,msgId,f))
                                    }
                                    val names = parseStructNames(tlvType.structDef.fields,sqls(0))
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_ROW,row.toInt,0,names)
                                    resdefs += resdef
                                case reg4() =>
                                    val tlvTypeName = codec.msgKeyToTypeMapForRes.getOrElse(msgId,EMPTY_STRINGMAP).getOrElse(f,null)
                                    if( tlvTypeName == null ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                                    }
                                    val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName,null)
                                    if( tlvType == null ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId,msgId,f))
                                    }
                                    if( tlvType.cls != TlvType.CLS_STRUCTARRAY && tlvType.cls != TlvType.CLS_OBJECTARRAY) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s, the tlv type must struct".format(serviceId,msgId,f))
                                    }
                                    val names = parseStructNames(tlvType.structDef.fields,sqls(0))
                                    val resdef = new ResultDefine(f,MsgDefine.RESULTTYPE_ALL,0,0,names)
                                    resdefs += resdef
                                case _ =>
                                    throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,from=%s".format(serviceId,msgId,fromValue))

                            }

                        }
                    }

                    if( hasArrayReqFields ) {

                        if( sqlType == MsgDefine.SQLTYPE_UPDATE ) {
                            sqlType = MsgDefine.SQLTYPE_BATCHUPDATE
                        } else {
                            throw new RuntimeException("array parameter not allowed as request parameter for serviceId=%d,msgId=%d".format(serviceId,msgId))
                        }

                    }

                    val msgSqls = new ArrayBuffer[MsgSql]

                    for(sql <- sqls) {
                        val (newsql, keys, paramNames, paramTypes, overwriteBuff) = prepareSql(sql,sqlType,reqMapOrigBuff)
                        msgSqls += new MsgSql(newsql, keys, paramNames, paramTypes, overwriteBuff)
                    }

                    val msgdef = new MsgDefine(sqlType,msgSqls,resdefs)

                        //log.warn("serviceId-msgId="+serviceId+"-"+msgId+", msgdef="+msgdef.toString())

                    msgMap.put(serviceId+"-"+msgId,msgdef)
                }
            }

        }

        driver = createDriver(dbConfig)

    }

    def parseStructNames(structFields: ArrayBuffer[StructField],sql:String):List[String] = {
        var fields = sqlSelectFieldsMap.getOrElse(sql,null)
        if( fields == null ) {
            parseSelectFields(sql)
            fields = sqlSelectFieldsMap.getOrElse(sql,null)
        }
        if( fields == null || fields.size == 0 || fields.size < structFields.size ) {
            throw new RuntimeException("struct name mapping not valid, sql="+sql)
        }
        for(f <- structFields) {
            val key = f.name
            val idx = findIndexInSelect(sql, MsgDefine.SQLTYPE_SELECT,key)
            if( idx == -1 ) {
                throw new RuntimeException("struct name ["+key+"] mapping not valid, sql="+sql)
            }
        }

        val names = new ArrayBufferString()
        for(s <- fields) {
            var i = 0
            var found = false
            while( i < structFields.size && !found ) {
                if( s == structFields(i).name.toLowerCase ) { names += structFields(i).name; found = true; }
                i += 1
            }
            if( !found ) {
                names += s
            }
        }

        names.toList
    }

    def removeLastStr(key:String,rmStr:String):String = {
        if( key.endsWith(rmStr) )
            key.substring(0,key.lastIndexOf(rmStr))
        else
            null
    }

    def findIndexInSelectForList(sql:String,sqlType:Int,key:String):Int = {
        var col = findIndexInSelect(sql,sqlType,key)
        if( col >= 0 ) return col
        var s = removeLastStr(key,"s")
        if( s != null ) {
            var col = findIndexInSelect(sql,sqlType,s)
            if( col >= 0 ) return col
        }
        s = removeLastStr(key,"array")
        if( s != null ) {
            var col = findIndexInSelect(sql,sqlType,s)
            if( col >= 0 ) return col
        }
        s = removeLastStr(key,"_array")
        if( s != null ) {
            var col = findIndexInSelect(sql,sqlType,s)
            if( col >= 0 ) return col
        }
        s = removeLastStr(key,"list")
        if( s != null ) {
            var col = findIndexInSelect(sql,sqlType,s)
            if( col >= 0 ) return col
        }
        s = removeLastStr(key,"_list")
        if( s != null ) {
            var col = findIndexInSelect(sql,sqlType,s)
            if( col >= 0 ) return col
        }
        -1
    }

    def findIndexInSelect(sql:String,sqlType:Int,key:String):Int = {

        if( sqlType != MsgDefine.SQLTYPE_SELECT ) return -1

        var fields = sqlSelectFieldsMap.getOrElse(sql,null)
        if( fields == null ) {
            parseSelectFields(sql)
            fields = sqlSelectFieldsMap.getOrElse(sql,null)
        }
        if( fields == null || fields.size == 0) {
            return -1
        }
        val loweredkey = key.toLowerCase
        var i=0
        while( i< fields.size ) {
            if( loweredkey == fields(i) ) return i
            i += 1
        }

        -1
    }

    // select messageid,taskid,appid,deviceid,userid,receiver,date_format(createtime,'%Y-%m-%d %T') as createtime from 
    // attention: there is a , in date_format function

    def parseSelectFields(sql:String) {
        var s = sql.toLowerCase
        val p1 = s.indexOf(" return ")
        if( p1 < 0 ) return
        s = s.substring(p1+8)
        var p2 = s.indexOf(" order by ")
        if( p2 >= 0 ) {
            s = s.substring(0,p2)
        } else {
            p2 = s.indexOf(" skip ")
            if( p2 >= 0 ) { 
                s = s.substring(0,p2)
            } else {
                p2 = s.indexOf(" limit ")
                if( p2 >= 0 ) s = s.substring(0,p2)
            }
        }

        var ss = s.split(",")
        if( s.indexOf("(") >= 0 ) { // include function
            ss = splitFieldWithFunction(s)
        }
        val fields = new ArrayBuffer[String]()
        for( m <- ss ) {
            fields += parseFieldName(m)
        }

        sqlSelectFieldsMap.put(sql,fields)
    }

    // attention: more than two '(' in function not supported
    def splitFieldWithFunction(s:String):scala.Array[String] = {
        val r1 = """'[^']*'""".r  
        val b2 = """\([^)]*\([^)]*\)[^)]*\)""".r
        val b1 = """\([^)]*\)""".r 
        var ns = r1.replaceAllIn(s,"") // remove '' 
        ns = b2.replaceAllIn(ns,"") // remove (())
        ns = b1.replaceAllIn(ns,"") // remove ()
        ns.split(",")
    }

    def parseFieldName(field:String):String = {
        val trimedfield = field.trim.replace("`","") // ` is a special char in mysql
        val p1 = trimedfield.lastIndexOf(" ")
        if( p1 < 0 ) {
            val p2 = trimedfield.lastIndexOf(".")
            if( p2 < 0 ) {
                trimedfield
            } else {
                trimedfield.substring(p2+1)
            }
        } else {
            trimedfield.substring(p1+1)
        }
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301002

        if( hasError(driver) ) {
            val msg = "neo4j db has error"
            buff += new SelfCheckResult("SCALABPE.DB",errorId,true,msg)
        }

        if( buff.size == 0 ) {
            buff += new SelfCheckResult("SCALABPE.DB",errorId)
        }

        buff
    }

    def createDriver(dbConfig:DbConfig):Driver = {
        val connStr = parseDbConnStr(dbConfig.connStr)
        val ds = createDriver(connStr.jdbcString,connStr.username,connStr.password,dbConfig.connNum)
        ds
    }

    def prepareSql(sql:String, sqlType:Int, params: ArrayBuffer[RequestDefineOriginal]) : Tuple5[String,ArrayBuffer[RequestDefine],ArrayBufferString,ArrayBuffer[Int],ArrayBuffer[RequestDefineOverwrite] ]= {

        val keys = new ArrayBuffer[RequestDefine]()
        val paramNames = new ArrayBufferString()
        val paramTypes = new ArrayBuffer[Int]()

        val overwriteBuff = new ArrayBuffer[RequestDefineOverwrite]()
        val overwriteMatchlist = sqlReg2Overwrite.findAllMatchIn(sql)
        if( overwriteMatchlist != null ) {

            val tags = overwriteMatchlist.toList
            for(tag <- tags ) {

                    tag.matched match {
                        case sqlReg3Overwrite(idx) =>
                            // sql overwrite doesnot support batch update
                            val ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase) // compare placeholder and request key
                            if( ss.size != 1 )
                                throw new RuntimeException("msg define error, sql="+sql+",tag="+tag)
                            val ss0 = ss(0)
                        overwriteBuff += new RequestDefineOverwrite(ss0.key,idx,ss0.defaultValue)
                        case _ =>
                            log.error("unknown match string {}",tag.matched)
                            throw new RuntimeException("sql define error")
                    }

            }

        }

        val matchlist = sqlReg2.findAllMatchIn(sql)
        if( matchlist == null ) {
            val tp = (sql,keys,paramNames,paramTypes,overwriteBuff)
            return tp
        }
        val tags = matchlist.toList
        for(tag <- tags ) {

            tag.matched match {
                case sqlReg3(idx) =>
                    var ss : ArrayBuffer[RequestDefineOriginal] = null

                    if( sqlType == MsgDefine.SQLTYPE_BATCHUPDATE ) {
                        ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase) // compare placeholder and request key
                        if( ss.size == 0 )
                            ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase + "s")
                        if( ss.size == 0 )
                            ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase + "array")
                        if( ss.size == 0 )
                            ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase + "_array")
                        if( ss.size == 0 )
                            ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase + "list")
                        if( ss.size == 0 )
                            ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase + "_list")
                    } else {
                        ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase) // compare placeholder and request key
                    }

                    if( ss.size != 1 )
                        throw new RuntimeException("msg define error, sql="+sql+",tag="+tag)
                    val ss0 = ss(0)
                    keys += new RequestDefine(ss0.key,ss0.defaultValue)
                    paramNames += idx
                    paramTypes += ColumnType.toColumnType(ss0.columnType)
                case _ =>
                    log.error("unknown match string {}",tag.matched)
                    throw new RuntimeException("sql define error")
            }

        }

        (sql,keys,paramNames,paramTypes,overwriteBuff)
    }

    def splitSql(sql:String):ArrayBufferString = {

        var trimedsql = sql.trim()
        var sss = trimedsql.replace("\n\r","#####").replace("\r","#####").replace("\n","#####").split("#####")
        val bs = new ArrayBufferString()
        for(s <- sss ) {
            val trimeds = s.trim()
            bs += trimeds
        }
        if( bs.size < 2 ) return bs

        // allow one sql multiple lines 
        // split sql by empty line

        val nbs = new ArrayBufferString()
        var tempsql = bs(0)
        for(i <- 1 until bs.size ) {
            if( bs(i) == "" ) {
                if( tempsql != "" ) {
                    nbs += tempsql ; 
                    tempsql = bs(i) 
                }
            } else {
                tempsql += " " + bs(i)
            }
        }
        if( tempsql != "" ) nbs += tempsql
        nbs
    }

    def generateSql(msgSql:MsgSql,req:Request) :String = {

        var sql = msgSql.sql

        if( msgSql.overwriteBuff != null ) {
            var i = 0
            while( i < msgSql.overwriteBuff.size) {

                val rdo = msgSql.overwriteBuff(i)

                val v = req.s(rdo.key,rdo.defaultValue)
                if( v == null ) {
                    throw new RuntimeException("replace field cannot be null, field=$"+rdo.idx)
                }

                sql = sql.replace("$"+rdo.idx,v)

                i += 1
            }

        }

        sql
    }

    def replaceSql(msgSql:MsgSql,req:Request) : Tuple2[String,ArrayBufferString]= {

        val sql = generateSql(msgSql,req)

        val values = new ArrayBufferString()

        for( keydef <- msgSql.keys ) {
            val value = req.s(keydef.key,keydef.defaultValue)
            values += value
        }

        (sql,values)
    }

    def replaceSqlBatch(msgSql:MsgSql,req:Request) : Tuple2[String,ArrayBuffer[ ArrayBufferString] ]= {

        val sql = generateSql(msgSql,req)

        val batchparams = new ArrayBuffer[ ArrayBufferString ] ()

        val buff_any = new ArrayBuffer[ Any ] ()
        val buff_size = new ArrayBuffer[ Int ] ()

        for( keydef <- msgSql.keys ) {
            val value = req.body.getOrElse(keydef.key,null)
            buff_any += value
            if( value == null ) buff_size += 1
            else {
                value match {
                    case s:String => buff_size += 1
                    case i:Int => buff_size += 1
                    case a:ArrayBufferString => buff_size += a.size
                    case a:ArrayBufferInt => buff_size += a.size
                    case _ =>
                        throw new RuntimeException("not supported type in request parameter")
                }
            }
        }

        var maxRows = 1
        var i = 0
        while( i< buff_size.size) {
            if( buff_size(i) > 1 ) {
                if( buff_size(i) > maxRows ) {
                    if( maxRows == 1 ) maxRows = buff_size(i)
                    else throw new RuntimeException("batch update parameter size not match")
                }
            }
            i+=1
        }

        var kk = 0

        while( kk < maxRows ) {
            val values = new ArrayBufferString()

            var mm = 0
            while( mm < buff_any.size ) {

                val v = buff_any(mm)
                if( v == null ) {

                    values += msgSql.keys(mm).defaultValue

                } else {

                    v match  {
                        case s:String => values += s
                        case i: Int => values += i.toString
                        case a:ArrayBufferString => values += a(kk)
                        case a:ArrayBufferInt => values += a(kk).toString
                    }

                }

                mm += 1
            }

            kk += 1
            batchparams += values
        }

        (sql,batchparams)
    }

    def process(req:Request):Unit = {

        val msgDefine = msgMap.getOrElse(req.serviceId+"-"+req.msgId,null)
        if( msgDefine == null ) {
            reply(req,ResultCodes.DB_ERROR)
            return
        }

        if( mode == DbLike.MODE_SYNC ) {

            try {
                req.msgId match {

                    case DbLike.BEGINTRANSACTION =>
                        beginTransaction(driver)
                        reply(req,0)
                        return
                    case DbLike.COMMIT =>
                        commit()
                        reply(req,0)
                        return
                    case DbLike.ROLLBACK =>
                        rollback()
                        reply(req,0)
                        return
                    case _ =>
                        checkBindConnection()

                }

            } catch {
                case e:Throwable =>
                    log.error("db exception, e=%s".format(e.getMessage))
                    val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                    val body = new HashMapStringAny()
                    for( resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_SQLCODE) {
                        body.put(resdef.key,sqlCode)
                    }
                    val errorCode = ErrorCodeUtils.parseErrorCode(e)
                    reply(req,errorCode,body)
                    return
            }

        }

        var results : DbResults = null

        msgDefine.sqlType match {


            case MsgDefine.SQLTYPE_SELECT =>

                val (sql,params) = replaceSql(msgDefine.sqls(0),req)
                val paramNames = msgDefine.sqls(0).paramNames
                val paramTypes = msgDefine.sqls(0).paramTypes
                
                val saveToFile = req.ns("saveToFile")
                val saveToFileSplitter = req.ns("saveToFileSplitter",",")
                if( saveToFile == "")
                    results = query_db(sql,params,paramNames,paramTypes,driver)
                else
                    results = query_db_to_file(sql,params,paramNames,paramTypes,driver,saveToFile,saveToFileSplitter)

            case MsgDefine.SQLTYPE_UPDATE =>

                val (sql,params) = replaceSql(msgDefine.sqls(0),req)
                val paramNames = msgDefine.sqls(0).paramNames
                val paramTypes = msgDefine.sqls(0).paramTypes

                results = update_db(sql,params,paramNames,paramTypes,driver)

            case MsgDefine.SQLTYPE_MULTIUPDATE =>

                val sql_buff= new ArrayBuffer[String]()
                val params_buff = new ArrayBuffer[ArrayBufferString]()
                val paramNames_buff = new ArrayBuffer[ArrayBufferString]()
                val paramTypes_buff = new ArrayBuffer[ArrayBuffer[Int]]()

                for(msgSql <- msgDefine.sqls ) {
                    val (sql,params) = replaceSql(msgSql,req)
                    sql_buff += sql
                    params_buff += params
                    paramNames_buff += msgSql.paramNames
                    paramTypes_buff += msgSql.paramTypes
                }

                results = update_db_multi(sql_buff,params_buff,paramNames_buff,paramTypes_buff,driver)

            case MsgDefine.SQLTYPE_BATCHUPDATE =>

                val (sql,batchparams) = replaceSqlBatch(msgDefine.sqls(0),req)
                val paramNames = msgDefine.sqls(0).paramNames
                val paramTypes = msgDefine.sqls(0).paramTypes

                results = update_db_batch(sql,batchparams,paramNames,paramTypes,driver)

            case _ =>
                val body = new HashMapStringAny()
                reply(req,ResultCodes.DB_ERROR,body)
                return
        }

        if( results.rowCount == -1 ) {

            val body = new HashMapStringAny()
            for( resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_SQLCODE) {
                body.put(resdef.key,results.sqlCode)
            }

            val errorCode = ErrorCodeUtils.sqlCodeToErrorCode(results.sqlCode)
            reply(req,errorCode,body)
            return
        }

        val body = new HashMapStringAny()
        for( resdef <- msgDefine.resdefs ) {

            resdef.fieldType match {

                case MsgDefine.RESULTTYPE_ROWCOUNT =>

                    body.put(resdef.key,results.rowCount)

                case MsgDefine.RESULTTYPE_COLUMN =>

                    body.put(resdef.key,results.value(resdef.row,resdef.col))

                case MsgDefine.RESULTTYPE_COLUMNLIST =>

                    body.put(resdef.key,results.valueList(resdef.col))

                case MsgDefine.RESULTTYPE_ROW =>

                    body.put(resdef.key,results.value(resdef.row,resdef.rowNames))

                case MsgDefine.RESULTTYPE_ALL =>

                    body.put(resdef.key,results.all(resdef.rowNames))

                case MsgDefine.RESULTTYPE_SQLCODE =>

                    body.put(resdef.key,0)

                case _ =>

                    ;
            }
        }

        reply(req,0,body)
    }

    def reply(req:Request, code:Int) :Unit ={
        reply(req,code,new HashMapStringAny())
    }

    def reply(req:Request, code:Int,params:HashMapStringAny):Unit = {

        val res = new Response(code,params,req)
        dbActor.reply(new RequestResponseInfo(req,res))
    }

}

