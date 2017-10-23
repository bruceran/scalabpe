package scalabpe.plugin

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import javax.sql.DataSource
import scalabpe.core.Actor
import scalabpe.core.ArrayBufferInt
import scalabpe.core.ArrayBufferLong
import scalabpe.core.ArrayBufferDouble
import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.Closable
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.HashMapStringString
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.Router
import scalabpe.core.SelfCheckLike
import scalabpe.core.SelfCheckResult
import scalabpe.core.SyncedActor
import scalabpe.core.TlvCodec
import scalabpe.core.TlvType
import scalabpe.core.TypeSafe
import scalabpe.core.StructField

object MsgDefine {

    val SQLTYPE_SELECT = 1
    val SQLTYPE_UPDATE = 2
    val SQLTYPE_MULTIUPDATE = 3
    val SQLTYPE_BATCHUPDATEBYCOL = 4
    val SQLTYPE_TRANSACTION = 5
    val SQLTYPE_UNKNOWN = 6
    val SQLTYPE_BATCHUPDATEBYROW = 7

    val REPLACE_TYPE_UNKNOWN = 0
    val REPLACE_TYPE_PREPARE = 1
    val REPLACE_TYPE_OVERWRITE = 2

    val selectReg = """^select .*$""".r
    val select2Reg = """^/\*[^/]*/[ ]*select .*$""".r
    val insertReg = """^insert .*$""".r
    val updateReg = """^update .*$""".r
    val deleteReg = """^delete .*$""".r
    val mergeReg = """^merge .*$""".r
    val createReg = """^create .*$""".r
    val alterReg = """^alter .*$""".r
    val dropReg = """^drop .*$""".r
    val replaceReg = """^replace .*$""".r // only valid in mysql
    val spSelectReg = """^[?]=[ ]*call .*$""".r
    val spUpdateReg = """^call .*$""".r
    val spSelectReg2 = """^[{][?]=[ ]*call .*[}]$""".r
    val spUpdateReg2 = """^[{]call .*[}]$""".r

    def toSqlType(sql: String): Int = {

        val trimedsql = sql.trim()

        val prefix = trimedsql.toLowerCase
        prefix match {
            case selectReg()    => MsgDefine.SQLTYPE_SELECT
            case select2Reg()   => MsgDefine.SQLTYPE_SELECT
            case insertReg()    => MsgDefine.SQLTYPE_UPDATE
            case updateReg()    => MsgDefine.SQLTYPE_UPDATE
            case deleteReg()    => MsgDefine.SQLTYPE_UPDATE
            case mergeReg()     => MsgDefine.SQLTYPE_UPDATE
            case createReg()    => MsgDefine.SQLTYPE_UPDATE
            case alterReg()     => MsgDefine.SQLTYPE_UPDATE
            case dropReg()      => MsgDefine.SQLTYPE_UPDATE
            case replaceReg()   => MsgDefine.SQLTYPE_UPDATE
            case spSelectReg()  => MsgDefine.SQLTYPE_SELECT
            case spSelectReg2() => MsgDefine.SQLTYPE_SELECT
            case spUpdateReg()  => MsgDefine.SQLTYPE_UPDATE
            case spUpdateReg2() => MsgDefine.SQLTYPE_UPDATE
            case _              => MsgDefine.SQLTYPE_UNKNOWN
        }
    }

    val RESULTTYPE_ROWCOUNT = 1
    val RESULTTYPE_ROW = 2
    val RESULTTYPE_COLUMN = 3
    val RESULTTYPE_ALL = 4
    val RESULTTYPE_SQLCODE = 5
    val RESULTTYPE_COLUMNLIST = 6
    val RESULTTYPE_INSERT_ID = 7
    val RESULTTYPE_INSERT_IDS = 8

    val SPLITTABLE_NODEFINE = 0
    val SPLITTABLE_ASSIGN = 1
    val SPLITTABLE_TAIL1 = 2
    val SPLITTABLE_TAIL2 = 3
    val SPLITTABLE_MOD = 4
    val SPLITTABLE_MODPAD0 = 5
    val SPLITTABLE_CUSTOM = 6

    def toSplitTableType(s: String): Int = {
        s.toLowerCase() match {
            case "assign"   => SPLITTABLE_ASSIGN
            case "tail1"    => SPLITTABLE_TAIL1
            case "tail2"    => SPLITTABLE_TAIL2
            case "mod"      => SPLITTABLE_MOD
            case "modpad0"  => SPLITTABLE_MODPAD0
            case "custom"   => SPLITTABLE_CUSTOM
            case "nodefine" => SPLITTABLE_NODEFINE
            case _          => SPLITTABLE_ASSIGN
        }
    }

    val SPLITDB_NO = 1
    val SPLITDB_ASSIGN = 2
    val SPLITDB_CUSTOM = 3

    def toSplitDbType(s: String): Int = {
        s.toLowerCase() match {
            case "no"     => SPLITDB_NO
            case "assign" => SPLITDB_ASSIGN
            case "custom" => SPLITDB_CUSTOM
            case _        => SPLITDB_NO
        }
    }

}

class DbConfig(
        val connNum: Int,
        val defaultConnStr: String,
        val connStrs: List[String],

        val splitTableType: Int = MsgDefine.SPLITTABLE_ASSIGN,
        val tbFactor: Int = 1,
        val splitTableCustomCls: String = null,

        val splitDbType: Int = MsgDefine.SPLITDB_NO,
        val dbFactor: Int = 1,
        val splitDbCustomCls: String = null) {

    var splitTablePlugin: SplitTablePlugin = null
    var splitDbPlugin: SplitDbPlugin = null

    override def toString() = {
        ("connNum=%d,defaultConnStr=%s,connStr=%s,splitTableType=%d,tbFactor=%d,splitTableCustomCls=%s," +
            "splitDbType=%d,dbFactor=%d,splitDbCustomCls=%s").format(
                connNum, defaultConnStr, connStrs.mkString("###"),
                splitTableType, tbFactor, splitTableCustomCls,
                splitDbType, dbFactor, splitDbCustomCls)
    }

}

class DbActor(override val router: Router, override val cfgNode: Node) extends BaseDbActor(router, cfgNode, DbLike.MODE_ASYNC) {

    init

    override def init() {
        super.init
    }

    override def receive(v: Any): Unit = {

        v match {

            case req: Request =>

                // log.info("request received, req={}",req.toString)
                try {

                    pool.execute(new Runnable() {
                        def run() {

                            val now = System.currentTimeMillis
                            if (req.receivedTime + req.expireTimeout < now) {
                                db.reply(req, ResultCodes.SERVICE_BUSY)
                                log.error("db is busy, req expired, req={}", req)
                                return
                            }

                            try {
                                db.process(req)
                            } catch {
                                case e: Exception =>
                                    val errorCode = ErrorCodeUtils.parseErrorCode(db.masterList(0), e)
                                    db.reply(req, errorCode)
                                    log.error("db exception req={}", req, e)
                            }

                        }
                    })

                } catch {
                    case e: RejectedExecutionException =>
                        db.reply(req, ResultCodes.SERVICE_FULL)
                        log.error("db queue is full, serviceIds={}", serviceIds)
                }

            case _ =>

                log.error("unknown msg")

        }
    }

    def reply(reqResInfo: RequestResponseInfo): Unit = {
        router.reply(reqResInfo)
    }

}

class SyncedDbActor(override val router: Router, override val cfgNode: Node) extends BaseDbActor(router, cfgNode, DbLike.MODE_SYNC) with SyncedActor {

    val retmap = new ConcurrentHashMap[String, Response]()

    init

    override def init() {
        super.init
    }

    override def receive(v: Any): Unit = {

        v match {

            case req: Request =>

                val now = System.currentTimeMillis
                if (req.receivedTime + req.expireTimeout < now) {
                    db.reply(req, ResultCodes.SERVICE_BUSY)
                    log.error("db is busy, req expired, req={}", req)
                    return
                }

                try {
                    db.process(req) // use the caller's thread to do db operation
                } catch {
                    case e: Exception =>
                        val errorCode = ErrorCodeUtils.parseErrorCode(db.masterList(0), e)
                        db.reply(req, errorCode)
                        log.error("db exception req={}", req, e)
                }

            case _ =>

                log.error("unknown msg")
        }
    }

    def reply(reqResInfo: RequestResponseInfo): Unit = {

        val (newbody, ec) = router.encodeResponse(reqResInfo.res.serviceId, reqResInfo.res.msgId, reqResInfo.res.code, reqResInfo.res.body)
        var errorCode = reqResInfo.res.code
        if (errorCode == 0 && ec != 0) {
            errorCode = ec
        }

        val res = new Response(errorCode, newbody, reqResInfo.req)
        retmap.put(reqResInfo.req.requestId, res)
    }

    def get(requestId: String): Response = {
        retmap.remove(requestId)
    }
}

abstract class BaseDbActor(val router: Router, val cfgNode: Node, val dbMode: Int) extends Actor with Logging with Closable with SelfCheckLike with Dumpable {

    var serviceIds: String = _
    var db: DbClient = _

    val queueSize = 20000
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _

    def dump() {

        log.info("--- serviceIds=" + serviceIds)

        if (pool != null) {
            val buff = new StringBuilder

            buff.append("pool.size=").append(pool.getPoolSize).append(",")
            buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

            log.info(buff.toString)
        }

        db.dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text

        var masterConfig: DbConfig = null
        var slaveConfig: DbConfig = null

        var s: String = null

        val masterNode = (cfgNode \ "MasterDb")
        if (!masterNode.isEmpty) {
            val conns = (masterNode \ "@conns").text.toInt

            s = (masterNode \ "@splitTableType").text
            val splitTableType = MsgDefine.toSplitTableType(s)
            var splitTableCustomCls: String = null
            s = (masterNode \ "@splitTableCustomCls").text
            if (splitTableType == MsgDefine.SPLITTABLE_CUSTOM) {
                if (s == "") throw new RuntimeException("splitTableCustomCls must be defined for splitTableType=custom")
            }
            if (s != "") splitTableCustomCls = s
            s = (masterNode \ "@tbfactor").text
            val tbfactor = if (s == "") 1 else s.toInt

            s = (masterNode \ "@splitDbType").text
            var splitDbType = MsgDefine.toSplitDbType(s)
            var splitDbCustomCls: String = null
            if (splitDbType == MsgDefine.SPLITDB_CUSTOM) {
                s = (masterNode \ "@splitDbCustomCls").text
                if (s == "") throw new RuntimeException("splitDbCustomCls must be defined for splitDbType=custom")
                splitDbCustomCls = s
            }
            s = (masterNode \ "@dbfactor").text
            var dbfactor = if (s == "") 1 else s.toInt

            var defaultConnStr = (masterNode \ "defaultconn").text
            if (defaultConnStr == "") defaultConnStr = (masterNode \ "DefaultConn").text
            if (defaultConnStr != "") {

                splitDbType = MsgDefine.SPLITDB_NO
                dbfactor = 1
                splitDbCustomCls = null

                masterConfig = new DbConfig(conns, defaultConnStr, null,
                    splitTableType, tbfactor, splitTableCustomCls,
                    splitDbType, dbfactor, splitDbCustomCls)

            } else {
                var connStrs = (masterNode \ "DivideConns" \ "conn").map(_.text).toList
                if (connStrs.size == 0)
                    connStrs = (masterNode \ "DivideConns" \ "Conn").map(_.text).toList

                if (dbfactor != connStrs.size) {
                    throw new RuntimeException("dbfactor must be %d".format(connStrs.size))
                }

                if (connStrs.size == 1) {
                    splitDbType = MsgDefine.SPLITDB_NO
                    dbfactor = 1
                    splitDbCustomCls = null
                }

                masterConfig = new DbConfig(conns, null, connStrs,
                    splitTableType, tbfactor, splitTableCustomCls,
                    splitDbType, dbfactor, splitDbCustomCls)

            }

            if (masterConfig.splitTableCustomCls != null) {

                try {

                    val obj = Class.forName(masterConfig.splitTableCustomCls).getConstructors()(0).newInstance()
                    if (!obj.isInstanceOf[SplitTablePlugin]) {
                        throw new RuntimeException("plugin %s is not SplitTablePlugin".format(masterConfig.splitTableCustomCls))
                    }
                    masterConfig.splitTablePlugin = obj.asInstanceOf[SplitTablePlugin]

                } catch {
                    case e: Exception =>
                        log.error("db plugin {} cannot be loaded", masterConfig.splitTableCustomCls)
                        throw e
                }
            }

            if (masterConfig.splitDbCustomCls != null) {

                try {

                    val obj = Class.forName(masterConfig.splitDbCustomCls).getConstructors()(0).newInstance()
                    if (!obj.isInstanceOf[SplitDbPlugin]) {
                        throw new RuntimeException("plugin %s is not SplitDbPlugin".format(masterConfig.splitDbCustomCls))
                    }
                    masterConfig.splitDbPlugin = obj.asInstanceOf[SplitDbPlugin]

                } catch {
                    case e: Exception =>
                        log.error("db plugin {} cannot be loaded", masterConfig.splitDbCustomCls)
                        throw e
                }
            }

        }

        val slaveNode = (cfgNode \ "SlaveDb")
        if (!slaveNode.isEmpty) {
            val conns = (slaveNode \ "@conns").text.toInt

            var defaultConnStr = (slaveNode \ "defaultconn").text
            if (defaultConnStr == "") defaultConnStr = (slaveNode \ "DefaultConn").text

            if (defaultConnStr != "") {
                slaveConfig = new DbConfig(conns, defaultConnStr, null)
            } else {
                var connStrs = (slaveNode \ "DivideConns" \ "conn").map(_.text).toList
                if (connStrs.size == 0)
                    connStrs = (slaveNode \ "DivideConns" \ "Conn").map(_.text).toList
                slaveConfig = new DbConfig(conns, null, connStrs)
            }
        }

        db = new DbClient(serviceIds, masterConfig, slaveConfig, router, this, dbMode)

        s = (router.cfgXml \ "LongTimeSql").text
        if (s != "")
            db.longTimeSql = s.toInt

        // println("longTimeSql="+db.longTimeSql)

        if (dbMode == DbLike.MODE_ASYNC) {

            var dbThreadNumStr = (cfgNode \ "@threadNum").text
            if (dbThreadNumStr == "") dbThreadNumStr = (router.cfgXml \ "dbthreadnum").text
            if (dbThreadNumStr == "") dbThreadNumStr = (router.cfgXml \ "DbThreadNum").text

            val dbThreadNum = dbThreadNumStr.toInt

            val firstServiceId = serviceIds.split(",")(0)
            threadFactory = new NamedThreadFactory("db" + firstServiceId)
            pool = new ThreadPoolExecutor(dbThreadNum, dbThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
            pool.prestartAllCoreThreads()

        }

        log.info(getClass.getName + " started {}", serviceIds)
    }

    def close() {

        if (pool != null) {

            val t1 = System.currentTimeMillis

            pool.shutdown()

            pool.awaitTermination(5, TimeUnit.SECONDS)

            val t2 = System.currentTimeMillis
            if (t2 - t1 > 100)
                log.warn(getClass.getName + " long time to shutdown pool, ts={}", t2 - t1)

            pool = null
        }

        db.close()
        log.info(getClass.getName + " stopped {}", serviceIds)
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {
        val buff = db.selfcheck()
        buff
    }

    def reply(reqResInfo: RequestResponseInfo): Unit
}

class RequestDefineOriginal(
        val key: String,
        val idx: String, // used to replace :idx
        val defaultValue: String = null,
        val columnType: String = null,
        val tableIdx: String = null, // used to replace $hashNum or $tableIdx
        val replaceType: Int = MsgDefine.REPLACE_TYPE_UNKNOWN,
        var structTlvType: TlvType = null) {
    override def toString() = {
        "key=%s,idx=%s,defaultValue=%s,columnType=%s,tableIdx=%s,replaceType=%d".format(key, idx, defaultValue, columnType, tableIdx, replaceType)
    }
}

class RequestDefine(
        val key: String,
        val defaultValue: String = null,
        var keyInMap: String = null,
        var defaultValueInMap: String = null) {
    override def toString() = {
        "key=%s,defaultValue=%s,keyInMap=%s,defaultValueInMap=%s".format(key, defaultValue, keyInMap, defaultValueInMap)
    }
}

class RequestDefineOverwrite(
        val key: String,
        val idx: String,
        val defaultValue: String = null) {
    override def toString() = {
        "key=%s,idx=%s,defaultValue=%s".format(key, idx, defaultValue)
    }
}

class MsgSql(
        val sql: String,
        val keys: ArrayBuffer[RequestDefine],
        val keyTypes: ArrayBuffer[Int],
        val tableIdx: String,
        val tableIdxField: String,
        val overwriteBuff: ArrayBuffer[RequestDefineOverwrite]) {

    override def toString() = {
        var keys_str = ""
        if (keys != null) keys_str = keys.mkString(",")
        var keyTypes_str = ""
        if (keyTypes != null) keyTypes_str = keyTypes.mkString(",")
        var overwriteBuff_str = ""
        if (overwriteBuff != null) overwriteBuff_str = overwriteBuff.mkString(",")
        "sql=%s,keys=%s,keyTypes=%s,tableIdx=%s,tableIdxField=%s,overwriteMap=%s".format(sql, keys_str, keyTypes_str, tableIdx, tableIdxField, overwriteBuff_str)
    }
}

class ResultDefine(
        val key: String,
        val fieldType: Int,
        val row: Int, // used when fieldType == TYPE_COLUMN | TYPE_ROW
        val col: Int, // used when fieldType == TYPE_COLUMN | TYPE_COLMUNLIST
        val rowNames: List[String]) { // used when fieldType == TYPE_ROW | TYPE_ALL, map to a avenue struct
    def this(key: String, fieldType: Int) {
        this(key, fieldType, 0, 0, List[String]())
    }
    def this(key: String, fieldType: Int, row: Int, col: Int) {
        this(key, fieldType, row, col, List[String]())
    }
    override def toString() = {
        "fieldType=%d,row=%d,col=%d,rowNames=%s".format(fieldType, row, col, rowNames.mkString(","))
    }
}

class MsgDefine(
        val sqlType: Int,
        val sqls: ArrayBuffer[MsgSql],
        val resdefs: ArrayBuffer[ResultDefine],
        val splitTableType: Int,
        val useSlave: Boolean = false) {

    override def toString() = {
        "sqlType=%d,sqls=%s,resdefs=%s,splitTableType=%d".format(sqlType, sqls.mkString(","), resdefs.mkString(","), splitTableType)
    }

}

object DbClient {

    val attackKeys = """script |exec |select |update |delete |insert |create |alter |drop |truncate """.split("\\|")

    val regOnlyTableIdx = """^\$(tableIdx)$""".r
    val regOnlyHashNum = """^\$(hashNum)$""".r
    val reg = """^:([0-9a-zA-Z_]+)$""".r
    val regWithTableIdx = """^:([0-9a-zA-Z_]+),\$([0-9a-zA-Z_]+)$""".r
    val regOverWrite = """^\$([0-9a-zA-Z_]+)$""".r
    val regWithTableIdxOverWrite = """^\$([0-9a-zA-Z_]+),\$([0-9a-zA-Z_]+)$""".r

    val reg1 = """^\$ROWCOUNT$""".r // $ROWCOUNT
    val reg2 = """^\$result\[([0-9]+)\]\[([0-9]+)\]$""".r // $result[n][m]
    val reg2list = """^\$result\[\*\]\[([0-9]+)\]$""".r // $result[*][m]
    val reg3 = """^\$result\[([0-9]+)\]$""".r // $result[n]
    val reg4 = """^\$result$""".r // $result
    val reg5 = """^\$SQLCODE$""".r // $SQLCODE
    val reg6 = """^\$insert_id$""".r // $insert_id
    val reg7 = """^\$insert_ids$""".r // $insert_ids

    // used to parse sql, replace :xxx to ?

    val sqlReg2 = """:[0-9a-zA-Z_]+[ ,)]|:[0-9a-zA-Z_]+$""".r // all sqlReg21 sqlReg22 sqlReg23 sqlReg24
    val sqlReg21 = """:[0-9a-zA-Z_]+[,]""".r // , after :placeholder
    val sqlReg22 = """:[0-9a-zA-Z_]+[ ]""".r // blank after :placeholder
    val sqlReg23 = """:[0-9a-zA-Z_]+[)]""".r // ) after :placeholder
    val sqlReg24 = """:[0-9a-zA-Z_]+$""".r // ends with :placeholder
    val sqlReg3 = """^:([0-9a-zA-Z_]+).*$""".r // :placeholder

    val sqlReg2Overwrite = """\$[0-9a-zA-Z_]+[ .,)]|\$[0-9a-zA-Z_]+$""".r
    val sqlReg3Overwrite = """^\$([0-9a-zA-Z_]+).*$""".r

    val EMPTY_STRINGMAP = new HashMapStringString()

}

class DbClient(
        val serviceIds: String,
        val masterDbConfig: DbConfig, // for insert,update,delete
        val slaveDbConfig: DbConfig, // for select
        val router: Router,
        val dbActor: BaseDbActor,
        val dbMode: Int) extends DbLike with Dumpable {

    import scalabpe.plugin.DbClient._

    mode = dbMode

    val msgMap = HashMap[String, MsgDefine]()

    var masterList: ArrayBuffer[DataSource] = null
    var slaveList: ArrayBuffer[DataSource] = null

    val sqlSelectFieldsMap = new HashMap[String, ArrayBuffer[String]]()

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("masterList.size=").append(masterList.size).append(",")
        masterList.foreach(dump)
        if (slaveList != null) {
            buff.append("slaveList.size=").append(slaveList.size).append(",")
            slaveList.foreach(dump)
        }

        log.info(buff.toString)
    }

    def close() {
        for (ds <- masterList)
            closeDataSource(ds)
        if (slaveList != null) {
            for (ds <- slaveList)
                closeDataSource(ds)
        }
    }

    def init() {

        if (masterDbConfig == null) throw new RuntimeException("masterDbConfig not defined")

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for (serviceId <- serviceIdArray) {
            val codec = router.codecs.findTlvCodec(serviceId)

            if (codec == null) {
                throw new RuntimeException("serviceId not found, serviceId=" + serviceId)
            }

            if (codec != null) {

                for ((msgId, map) <- codec.msgAttributes if isTransactionMsgId(msgId)) {

                    val resFields = codec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP).keys.toList
                    val resdefs = new ArrayBuffer[ResultDefine]()

                    for (f <- resFields) {

                        var fromValue = map.getOrElse("res-" + f + "-from", null)

                        if (fromValue == null) {
                            f.toLowerCase match {
                                case "sqlcode" =>
                                    fromValue = "$SQLCODE"
                                case _ =>
                                    throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,from=%s".format(serviceId, msgId, fromValue))
                            }
                        }
                        if (fromValue != null) {

                            fromValue match {

                                case reg5() =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_SQLCODE)
                                    resdefs += resdef
                                case _ =>
                                    throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,from=%s".format(serviceId, msgId, fromValue))

                            }

                        }
                    }

                    var sqlType = MsgDefine.SQLTYPE_TRANSACTION
                    val msgSqls = new ArrayBuffer[MsgSql]
                    msgSqls += new MsgSql("", null, null, null, null, null)
                    val msgdef = new MsgDefine(sqlType, msgSqls, resdefs, MsgDefine.SPLITTABLE_NODEFINE)

                    /*
                    if( log.isDebugEnabled() ) {
                        log.debug("msgdef="+msgdef.toString())
                    }
                     */

                    msgMap.put(serviceId + "-" + msgId, msgdef)
                }

                for ((msgId, map) <- codec.msgAttributes if !isTransactionMsgId(msgId)) {

                    val sql = map.getOrElse("sql", null)
                    if (sql == null) {
                        throw new RuntimeException("sql not defined for serviceId=%d,msgId=%d".format(serviceId, msgId))
                    }

                    var useSlaveAttr = map.getOrElse("useSlave", "")
                    if (useSlaveAttr == "") {
                        useSlaveAttr = "0"
                    }
                    var useSlave = TypeSafe.isTrue(useSlaveAttr)
                    //if( useSlave ) println("use slave, msgId="+msgId)

                    var splitTableTypeStr = map.getOrElse("splitTableType", "nodefine")
                    if (splitTableTypeStr == "") splitTableTypeStr = "nodefine"
                    val splitTableType = MsgDefine.toSplitTableType(splitTableTypeStr)

                    val sqls = splitSql(sql)

                    if (sqls.size == 0)
                        throw new RuntimeException("sql not defined for serviceId=%d,msgId=%d".format(serviceId, msgId))

                    var sqlType = MsgDefine.SQLTYPE_UNKNOWN
                    for (sql <- sqls) {
                        sqlType = MsgDefine.toSqlType(sql)
                        if (sqlType == MsgDefine.SQLTYPE_UNKNOWN)
                            throw new RuntimeException("sql is not valid,sql=%s".format(sql))
                        if (sqls.size > 1 && sqlType == MsgDefine.SQLTYPE_SELECT)
                            throw new RuntimeException("select is not allowed in multi sqls,sql=%s".format(sql))
                    }

                    if (sqls.size > 1)
                        sqlType = MsgDefine.SQLTYPE_MULTIUPDATE

                    val reqFields = codec.msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP).keys.toList
                    val reqMapOrigBuff = new ArrayBuffer[RequestDefineOriginal]()

                    var hasArrayReqFields = false
                    var hasArrayReqFieldsByRow = false
                    var hasArrayReqFieldsByCol = false

                    for (f <- reqFields) {

                        val tlvTypeName = codec.msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP).getOrElse(f, null)
                        if (tlvTypeName == null) {
                            throw new RuntimeException("key not found error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                        }
                        val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName, null)
                        if (tlvType == null) {
                            throw new RuntimeException("key not found error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                        }
                        if (tlvType.cls != TlvType.CLS_STRING &&
                            tlvType.cls != TlvType.CLS_STRINGARRAY &&
                            tlvType.cls != TlvType.CLS_INT &&
                            tlvType.cls != TlvType.CLS_INTARRAY &&
                            tlvType.cls != TlvType.CLS_LONG &&
                            tlvType.cls != TlvType.CLS_LONGARRAY &&
                            tlvType.cls != TlvType.CLS_DOUBLE &&
                            tlvType.cls != TlvType.CLS_DOUBLEARRAY &&
                            tlvType.cls != TlvType.CLS_STRUCTARRAY && 
                            tlvType.cls != TlvType.CLS_OBJECTARRAY )
                            throw new RuntimeException("type not supported in request parameter, serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))

                        if (tlvType.cls == TlvType.CLS_STRINGARRAY ||
                            tlvType.cls == TlvType.CLS_INTARRAY ||
                            tlvType.cls == TlvType.CLS_LONGARRAY ||
                            tlvType.cls == TlvType.CLS_DOUBLEARRAY ||
                            tlvType.cls == TlvType.CLS_STRUCTARRAY || 
                            tlvType.cls == TlvType.CLS_OBJECTARRAY)
                            hasArrayReqFields = true

                        if (tlvType.cls == TlvType.CLS_STRUCTARRAY || tlvType.cls == TlvType.CLS_OBJECTARRAY) {
                            if (hasArrayReqFieldsByCol || hasArrayReqFieldsByRow) {
                                throw new RuntimeException("msg define error in request parameter, serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                            }
                            hasArrayReqFieldsByRow = true
                        } else if (tlvType.cls == TlvType.CLS_STRINGARRAY || tlvType.cls == TlvType.CLS_INTARRAY || tlvType.cls == TlvType.CLS_LONGARRAY || tlvType.cls == TlvType.CLS_DOUBLEARRAY ) {
                            if (hasArrayReqFieldsByRow) {
                                throw new RuntimeException("msg define error in request parameter, serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                            }
                            hasArrayReqFieldsByCol = true
                        }

                        val defaultValue = map.getOrElse("req-" + f + "-default", null)
                        val columnType = map.getOrElse("req-" + f + "-columnType", "string")
                        val toValue = map.getOrElse("req-" + f + "-to", null)
                        if (toValue != null) {

                            toValue match {

                                case regOnlyTableIdx(tableIdx) =>
                                    val reqdef = new RequestDefineOriginal(f, f, defaultValue, columnType, tableIdx, MsgDefine.REPLACE_TYPE_UNKNOWN)
                                    reqMapOrigBuff += reqdef
                                case regOnlyHashNum(tableIdx) =>
                                    val reqdef = new RequestDefineOriginal(f, f, defaultValue, columnType, tableIdx, MsgDefine.REPLACE_TYPE_UNKNOWN)
                                    reqMapOrigBuff += reqdef
                                case reg(s) =>
                                    val reqdef = new RequestDefineOriginal(f, s, defaultValue, columnType, null, MsgDefine.REPLACE_TYPE_PREPARE)
                                    reqMapOrigBuff += reqdef
                                case regWithTableIdx(s, tableIdx) =>
                                    val reqdef = new RequestDefineOriginal(f, s, defaultValue, columnType, tableIdx, MsgDefine.REPLACE_TYPE_PREPARE)
                                    reqMapOrigBuff += reqdef
                                case regOverWrite(s) =>
                                    val reqdef = new RequestDefineOriginal(f, s, defaultValue, columnType, null, MsgDefine.REPLACE_TYPE_OVERWRITE)
                                    reqMapOrigBuff += reqdef
                                case regWithTableIdxOverWrite(s, tableIdx) =>
                                    val reqdef = new RequestDefineOriginal(f, s, defaultValue, columnType, tableIdx, MsgDefine.REPLACE_TYPE_OVERWRITE)
                                    reqMapOrigBuff += reqdef
                                case _ =>
                                    throw new RuntimeException("to defination error for serviceId=%d,msgId=%d,to=%s".format(serviceId, msgId, toValue))
                            }
                        } else {
                            val reqdef = new RequestDefineOriginal(f, f, defaultValue, columnType)
                            if (tlvType.cls == TlvType.CLS_STRUCTARRAY || tlvType.cls == TlvType.CLS_OBJECTARRAY ) {
                                reqdef.structTlvType = tlvType
                            }
                            reqMapOrigBuff += reqdef
                        }
                    }

                    val resFields = codec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP).keys.toList

                    val resdefs = new ArrayBuffer[ResultDefine]()

                    for (f <- resFields) {

                        var fromValue = map.getOrElse("res-" + f + "-from", null)
                        var ignoreCheck = map.getOrElse("res-" + f + "-ignoreCheck", "0") == "1"
                        var selectFields = map.getOrElse("res-" + f + "-selectFields", "")

                        if (fromValue == null) {

                            if (ignoreCheck) {
                                throw new RuntimeException("from must be defined with ignoreCheck for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                            }

                            f.toLowerCase match {

                                case "rowcount" =>
                                    fromValue = "$ROWCOUNT"

                                case "row_count" =>
                                    fromValue = "$ROWCOUNT"

                                case "sqlcode" =>
                                    fromValue = "$SQLCODE"

                                case "sql_code" =>
                                    fromValue = "$SQLCODE"

                                case "insert_id" =>
                                    fromValue = "$insert_id"

                                case "insert_ids" =>
                                    fromValue = "$insert_ids"

                                case _ =>

                                    val tlvTypeName = codec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP).getOrElse(f, null)
                                    if (tlvTypeName == null) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                                    }
                                    val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName, null)
                                    if (tlvType == null) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                                    }

                                    tlvType.cls match {

                                        case TlvType.CLS_STRING | TlvType.CLS_INT | TlvType.CLS_LONG | TlvType.CLS_DOUBLE =>

                                            val col = findIndexInSelect(sqls(0), sqlType, f)
                                            if (col == -1)
                                                throw new RuntimeException("from not defined for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))

                                            fromValue = "$result[0][%d]".format(col)

                                        case TlvType.CLS_STRUCT | TlvType.CLS_OBJECT =>

                                            fromValue = "$result[0]"

                                        case TlvType.CLS_STRINGARRAY | TlvType.CLS_INTARRAY  | TlvType.CLS_LONGARRAY  | TlvType.CLS_DOUBLEARRAY  =>

                                            val col = findIndexInSelectForList(sqls(0), sqlType, f)
                                            if (col == -1)
                                                throw new RuntimeException("from not defined for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))

                                            fromValue = "$result[*][%d]".format(col)

                                        case TlvType.CLS_STRUCTARRAY | TlvType.CLS_OBJECTARRAY =>
                                            fromValue = "$result"
                                    }

                            }

                        }

                        if (fromValue != null) {

                            fromValue match {

                                case reg1() =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_ROWCOUNT)
                                    resdefs += resdef
                                case reg5() =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_SQLCODE)
                                    resdefs += resdef
                                case reg6() =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_INSERT_ID)
                                    resdefs += resdef
                                case reg7() =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_INSERT_IDS)
                                    resdefs += resdef
                                case reg2(row, col) =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_COLUMN, row.toInt, col.toInt)
                                    resdefs += resdef
                                case reg2list(col) =>
                                    val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_COLUMNLIST, 0, col.toInt)
                                    resdefs += resdef
                                case reg3(row) =>
                                    val tlvTypeName = codec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP).getOrElse(f, null)
                                    if (tlvTypeName == null) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                                    }
                                    val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName, null)
                                    if (tlvType == null) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                                    }
                                    if (tlvType.cls != TlvType.CLS_STRUCT && tlvType.cls != TlvType.CLS_OBJECT ) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s, the tlv type must be struct/object".format(serviceId, msgId, f))
                                    }
                                    if (selectFields != "") {
                                        val names = selectFields.split(",").toList
                                        val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_ROW, row.toInt, 0, names)
                                        resdefs += resdef
                                    } else {
                                        val names = parseStructNames(tlvType.structDef.fields, sqls(0))
                                        val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_ROW, row.toInt, 0, names)
                                        resdefs += resdef
                                    }
                                case reg4() =>
                                    val tlvTypeName = codec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP).getOrElse(f, null)
                                    if (tlvTypeName == null) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                                    }
                                    val tlvType = codec.typeNameToCodeMap.getOrElse(tlvTypeName, null)
                                    if (tlvType == null) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s".format(serviceId, msgId, f))
                                    }
                                    if (tlvType.cls != TlvType.CLS_STRUCTARRAY && tlvType.cls != TlvType.CLS_OBJECTARRAY) {
                                        throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,key=%s, the tlv type must be struct/object".format(serviceId, msgId, f))
                                    }
                                    if (selectFields != "") {
                                        val names = selectFields.split(",").toList
                                        val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_ALL, 0, 0, names)
                                        resdefs += resdef
                                    } else {
                                        val names = parseStructNames(tlvType.structDef.fields, sqls(0))
                                        val resdef = new ResultDefine(f, MsgDefine.RESULTTYPE_ALL, 0, 0, names)
                                        resdefs += resdef
                                    }
                                case _ =>
                                    throw new RuntimeException("from defination error for serviceId=%d,msgId=%d,from=%s".format(serviceId, msgId, fromValue))

                            }

                        }
                    }

                    if (hasArrayReqFields) {

                        if (sqlType == MsgDefine.SQLTYPE_UPDATE) {
                            if (hasArrayReqFieldsByRow) {
                                sqlType = MsgDefine.SQLTYPE_BATCHUPDATEBYROW
                            } else {
                                sqlType = MsgDefine.SQLTYPE_BATCHUPDATEBYCOL
                            }
                        } else {
                            throw new RuntimeException("array parameter not allowed as request parameter for serviceId=%d,msgId=%d".format(serviceId, msgId))
                        }

                    }

                    val msgSqls = new ArrayBuffer[MsgSql]

                    for (sql <- sqls) {
                        val (newsql, keys, keytypes, tableIdx, tableIdxField, overwriteBuff) = prepareSql(sql, sqlType, reqMapOrigBuff, serviceId)
                        msgSqls += new MsgSql(newsql, keys, keytypes, tableIdx, tableIdxField, overwriteBuff)
                    }

                    val msgdef = new MsgDefine(sqlType, msgSqls, resdefs, splitTableType, useSlave)
                    /*
                    if( log.isDebugEnabled() ) {
                        log.debug("serviceId-msgId="+serviceId+"-"+msgId+", msgdef="+msgdef.toString())
                    }
                     */
                    msgMap.put(serviceId + "-" + msgId, msgdef)
                }
            }

        }

        masterList = createDataSources(masterDbConfig)
        if (slaveDbConfig != null) {
            slaveList = createDataSources(slaveDbConfig)
        }

        if (mode == DbLike.MODE_SYNC) {
            if (slaveDbConfig != null)
                throw new RuntimeException("master/slave not supported in SyncedDbActor")
            if (masterList.size > 1)
                throw new RuntimeException("only one datasource supported in SyncedDbActor")
        }
    }

    def parseStructNames(structFields: ArrayBuffer[StructField], sql: String): List[String] = {
        var fields = sqlSelectFieldsMap.getOrElse(sql, null)
        if (fields == null) {
            parseSelectFields(sql)
            fields = sqlSelectFieldsMap.getOrElse(sql, null)
        }
        if (fields == null || fields.size == 0 || fields.size < structFields.size) {
            throw new RuntimeException("struct name mapping not valid, sql=" + sql)
        }
        for (f <- structFields) {
            val key = f.name
            val idx = findIndexInSelect(sql, MsgDefine.SQLTYPE_SELECT, key)
            if (idx == -1) {
                throw new RuntimeException("struct name [" + key + "] mapping not valid, sql=" + sql)
            }
        }

        val names = new ArrayBufferString()
        for (s <- fields) {
            var i = 0
            var found = false
            while (i < structFields.size && !found) {
                if (s == structFields(i).name.toLowerCase) { names += structFields(i).name; found = true; }
                i += 1
            }
            if (!found) {
                names += s
            }
        }

        names.toList
    }

    def removeLastStr(key: String, rmStr: String): String = {
        if (key.endsWith(rmStr))
            key.substring(0, key.lastIndexOf(rmStr))
        else
            null
    }

    def findIndexInSelectForList(sql: String, sqlType: Int, key: String): Int = {
        var col = findIndexInSelect(sql, sqlType, key)
        if (col >= 0) return col
        var s = removeLastStr(key, "s")
        if (s != null) {
            var col = findIndexInSelect(sql, sqlType, s)
            if (col >= 0) return col
        }
        s = removeLastStr(key, "array")
        if (s != null) {
            var col = findIndexInSelect(sql, sqlType, s)
            if (col >= 0) return col
        }
        s = removeLastStr(key, "_array")
        if (s != null) {
            var col = findIndexInSelect(sql, sqlType, s)
            if (col >= 0) return col
        }
        s = removeLastStr(key, "list")
        if (s != null) {
            var col = findIndexInSelect(sql, sqlType, s)
            if (col >= 0) return col
        }
        s = removeLastStr(key, "_list")
        if (s != null) {
            var col = findIndexInSelect(sql, sqlType, s)
            if (col >= 0) return col
        }
        -1
    }

    def findIndexInSelect(sql: String, sqlType: Int, key: String): Int = {

        if (sqlType != MsgDefine.SQLTYPE_SELECT) return -1

        var fields = sqlSelectFieldsMap.getOrElse(sql, null)
        if (fields == null) {
            parseSelectFields(sql)
            fields = sqlSelectFieldsMap.getOrElse(sql, null)
        }
        if (fields == null || fields.size == 0) {
            return -1
        }
        val loweredkey = key.toLowerCase
        var i = 0
        while (i < fields.size) {
            if (loweredkey == fields(i)) return i
            i += 1
        }

        -1
    }

    // select messageid,taskid,appid,deviceid,userid,receiver,date_format(createtime,'%Y-%m-%d %T') as createtime from 
    // attention: there is a , in date_format function

    def parseSelectFields(sql: String) {
        val loweredsql = sql.toLowerCase
        val p1 = loweredsql.indexOf("select ")
        if (p1 < 0) return
        val p2 = loweredsql.indexOf(" from ")
        if (p2 < 0) return
        val s = loweredsql.substring(p1 + 7, p2)
        var ss = s.split(",")
        if (s.indexOf("(") >= 0) { // include function
            ss = splitFieldWithFunction(s)
        }
        val fields = new ArrayBuffer[String]()
        for (m <- ss) {
            fields += parseFieldName(m)
        }
        sqlSelectFieldsMap.put(sql, fields)
    }

    // attention: more than two '(' in function not supported
    def splitFieldWithFunction(s: String): scala.Array[String] = {
        val r1 = """'[^']*'""".r
        val b2 = """\([^)]*\([^)]*\)[^)]*\)""".r
        val b1 = """\([^)]*\)""".r
        var ns = r1.replaceAllIn(s, "") // remove '' 
        ns = b2.replaceAllIn(ns, "") // remove (())
        ns = b1.replaceAllIn(ns, "") // remove ()
        ns.split(",")
    }

    def parseFieldName(field: String): String = {
        val trimedfield = field.trim.replace("`", "") // ` is a special char in mysql
        val p1 = trimedfield.lastIndexOf(" ")
        if (p1 < 0) {
            val p2 = trimedfield.lastIndexOf(".")
            if (p2 < 0) {
                trimedfield
            } else {
                trimedfield.substring(p2 + 1)
            }
        } else {
            trimedfield.substring(p1 + 1)
        }
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301002

        for (ds <- masterList) {
            if (hasError(ds)) {
                val msg = "master db [" + DbLike.getUrl(ds) + "] has error"
                buff += new SelfCheckResult("SCALABPE.DB", errorId, true, msg)
            }
        }
        if (slaveList != null) {

            for (ds <- slaveList) {
                if (hasError(ds)) {
                    val msg = "slave db [" + DbLike.getUrl(ds) + "] has error"
                    buff += new SelfCheckResult("SCALABPE.DB", errorId, true, msg)
                }
            }

        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.DB", errorId)
        }

        buff
    }

    def createDataSources(dbConfig: DbConfig): ArrayBuffer[DataSource] = {

        val dslist = new ArrayBuffer[DataSource]()

        if (dbConfig.defaultConnStr != null && dbConfig.defaultConnStr != "") {
            val connStr = parseDbConnStr(dbConfig.defaultConnStr)
            val ds = createDataSource(connStr.jdbcString, connStr.username, connStr.password, dbConfig.connNum)
            dslist += ds
        } else {
            for (s <- dbConfig.connStrs) {
                val connStr = parseDbConnStr(s)
                val ds = createDataSource(connStr.jdbcString, connStr.username, connStr.password, dbConfig.connNum)
                dslist += ds
            }
        }
        dslist
    }

    def prepareSql(sql: String, sqlType: Int, params: ArrayBuffer[RequestDefineOriginal], serviceId: Int): Tuple6[String, ArrayBuffer[RequestDefine], ArrayBuffer[Int], String, String, ArrayBuffer[RequestDefineOverwrite]] = {

        val keys = new ArrayBuffer[RequestDefine]()
        val keytypes = new ArrayBuffer[Int]()

        var tableIdx: String = null
        var tableIdxField: String = null
        for (param <- params if param.tableIdx != null) {

            if (sql.indexOf("$" + param.tableIdx) >= 0) { // only first is valid

                if (tableIdx != null) {
                    throw new RuntimeException("only one tableIdx can be defined, tableIdx=%s".format(param.tableIdx))
                }

                tableIdx = param.tableIdx
                tableIdxField = param.key
            }

        }

        if (tableIdx == null) {
            if (sql.indexOf("$hashNum") >= 0) {
                tableIdx = "hashNum"
                tableIdxField = "hashNum"
            }
            if (sql.indexOf("$tableIdx") >= 0) {
                tableIdx = "tableIdx"
                tableIdxField = "tableIdx"
            }
        }

        val overwriteBuff = new ArrayBuffer[RequestDefineOverwrite]()
        val overwriteMatchlist = sqlReg2Overwrite.findAllMatchIn(sql)
        if (overwriteMatchlist != null) {

            val tags = overwriteMatchlist.toList
            for (tag <- tags) {

                if (tableIdx == null || tableIdx != null && tag.matched.indexOf(tableIdx) < 0) {

                    tag.matched match {
                        case sqlReg3Overwrite(idx) =>
                            // sql overwrite doesnot support batch update
                            val ss = params.filter(p => p.idx.toLowerCase == idx.toLowerCase) // compare placeholder and request key
                            if (ss.size != 1)
                                throw new RuntimeException("msg define error, sql=" + sql + ",tag=" + tag)
                            val ss0 = ss(0)
                            overwriteBuff += new RequestDefineOverwrite(ss0.key, idx, ss0.defaultValue)
                        case _ =>
                            log.error("unknown match string {}", tag.matched)
                            throw new RuntimeException("sql define error")
                    }

                }
            }

        }

        val matchlist = sqlReg2.findAllMatchIn(sql)
        if (matchlist == null) {
            val tp = (sql, keys, keytypes, tableIdx, tableIdxField, overwriteBuff)
            return tp
        }
        val codec = router.codecs.findTlvCodec(serviceId)
        val tags = matchlist.toList
        for (tag <- tags) {

            tag.matched match {
                case sqlReg3(idx) =>
                    var ss: ArrayBuffer[RequestDefineOriginal] = null

                    val loweredIdx = idx.toLowerCase
                    var keyInMap: String = null
                    var columnTypeInMap = ""
                    var defaultValueInMap: String = null

                    if (sqlType == MsgDefine.SQLTYPE_BATCHUPDATEBYCOL) {
                        ss = params.filter(p => p.idx.toLowerCase == loweredIdx) // compare placeholder and request key
                        if (ss.size == 0)
                            ss = params.filter(p => p.idx.toLowerCase == loweredIdx + "s")
                        if (ss.size == 0)
                            ss = params.filter(p => p.idx.toLowerCase == loweredIdx + "array")
                        if (ss.size == 0)
                            ss = params.filter(p => p.idx.toLowerCase == loweredIdx + "_array")
                        if (ss.size == 0)
                            ss = params.filter(p => p.idx.toLowerCase == loweredIdx + "list")
                        if (ss.size == 0)
                            ss = params.filter(p => p.idx.toLowerCase == loweredIdx + "_list")
                    } else if (sqlType == MsgDefine.SQLTYPE_BATCHUPDATEBYROW) {
                        ss = params.filter(p => p.structTlvType == null && p.idx.toLowerCase == loweredIdx) // compare placeholder and request key
                        if (ss.size == 0) {
                            ss = params.filter(p => p.structTlvType != null)
                            if (ss.size != 1)
                                throw new RuntimeException("msg define error, sql=" + sql + ",tag=" + tag)
                            val tlvType = ss(0).structTlvType
                            if( tlvType.cls == TlvType.CLS_STRUCTARRAY ) {
                                for (kk <- 0 until tlvType.structDef.fields.length) {
                                    if (tlvType.structDef.fields(kk).name.toLowerCase == loweredIdx) {
                                        keyInMap = tlvType.structDef.fields(kk).name
                                        columnTypeInMap = codec.codecAttributes.getOrElse("type-" + tlvType.itemType.name + "-" + keyInMap + "-columnType", "")
                                        val tlvinfo = tlvType.structDef.fields(kk).fieldInfo
                                        if (tlvinfo != null && tlvinfo.defaultValue != null) defaultValueInMap = tlvinfo.defaultValue
                                    }
                                }
                            }
                            if( tlvType.cls == TlvType.CLS_OBJECTARRAY ) {
                                for (kk <- 0 until tlvType.objectDef.fields.length) {
                                    val type_name = tlvType.objectDef.fields(kk).name
                                    val name = tlvType.objectDef.typeToKeyMap.getOrElse(type_name,"")
                                    if ( name.toLowerCase == loweredIdx) {
                                        keyInMap = name
                                        columnTypeInMap = codec.codecAttributes.getOrElse("type-" + tlvType.itemType.name + "-" + keyInMap + "-columnType", "")
                                        val tlvinfo = tlvType.objectDef.keyToFieldMap.getOrElse(keyInMap,null)
                                        if (tlvinfo != null && tlvinfo.defaultValue != null) defaultValueInMap = tlvinfo.defaultValue
                                    }
                                }
                            }
                            if (keyInMap == null) {
                                throw new RuntimeException("msg define error, sql=" + sql + ",tag=" + tag)
                            }
                        }
                    } else {
                        ss = params.filter(p => p.idx.toLowerCase == loweredIdx) // compare placeholder and request key
                    }

                    if (ss.size != 1)
                        throw new RuntimeException("msg define error, sql=" + sql + ",tag=" + tag)
                    val ss0 = ss(0)
                    val rd = new RequestDefine(ss0.key, ss0.defaultValue)
                    var tp = ColumnType.toColumnType(ss0.columnType)
                    if (keyInMap != null) {
                        rd.keyInMap = keyInMap
                        rd.defaultValueInMap = defaultValueInMap
                        tp = ColumnType.toColumnType(columnTypeInMap)
                    }
                    keys += rd
                    keytypes += tp
                case _ =>
                    log.error("unknown match string {}", tag.matched)
                    throw new RuntimeException("sql define error")
            }

        }

        var newsql = sqlReg21.replaceAllIn(sql, "?,")
        newsql = sqlReg22.replaceAllIn(newsql, "? ")
        newsql = sqlReg23.replaceAllIn(newsql, "?)")
        newsql = sqlReg24.replaceAllIn(newsql, "?")
        if (newsql.startsWith("?=")) {
            newsql = newsql.replace("?=", "")
        }
        if (newsql.startsWith("{?=")) {
            newsql = newsql.replace("{?=", "{")
        }

        (newsql, keys, keytypes, tableIdx, tableIdxField, overwriteBuff)
    }

    def splitSql(sql: String): ArrayBufferString = {

        var trimedsql = sql.trim()
        var sss = trimedsql.replace("\n\r", "#####").replace("\r", "#####").replace("\n", "#####").split("#####")
        val bs = new ArrayBufferString()
        for (s <- sss) {
            val trimeds = s.trim()
            if (trimeds != "") bs += trimeds
        }
        if (bs.size < 2) return bs

        // allow one sql multiple lines 
        val nbs = new ArrayBufferString()
        var tempsql = bs(0)
        for (i <- 1 until bs.size) {
            if (MsgDefine.toSqlType(bs(i)) == MsgDefine.SQLTYPE_UNKNOWN) tempsql += " " + bs(i)
            else { nbs += tempsql; tempsql = bs(i) }
        }
        if (tempsql != "") nbs += tempsql
        nbs
    }

    def generateTableIdx(splitTableType: Int, tableIdxField: String, req: Request): String = {

        val s = req.s(tableIdxField)
        if (s == null) return null

        splitTableType match {

            case MsgDefine.SPLITTABLE_ASSIGN =>
                return s
            case MsgDefine.SPLITTABLE_TAIL1 =>
                val idx = s.substring(s.length() - 1)
                return idx
            case MsgDefine.SPLITTABLE_TAIL2 =>
                val idx = s.substring(s.length() - 2)
                return idx
            case MsgDefine.SPLITTABLE_MOD =>
                val v = s.hashCode()
                val idx = String.valueOf(v % masterDbConfig.tbFactor)
                return idx
            case MsgDefine.SPLITTABLE_MODPAD0 =>
                val v = s.hashCode()
                val idx = String.valueOf(v % masterDbConfig.tbFactor)
                val padidx = if (idx.length() == 1) "0" + idx else idx
                return padidx

        }

        ""
    }

    def generateDbIdx(req: Request): Int = {

        if (masterDbConfig.splitDbType == MsgDefine.SPLITDB_NO)
            return 0

        if (masterDbConfig.splitDbType == MsgDefine.SPLITDB_CUSTOM) {
            val dbIdx = masterDbConfig.splitDbPlugin.generateDbIdx(req)

            if (dbIdx < 0 || dbIdx >= masterDbConfig.connStrs.size)
                throw new RuntimeException("db index is out of range dbIdx=%d".format(dbIdx))

            return dbIdx
        }
        var s = req.s("dbHashNum")
        if (s == null)
            s = req.s("dbIdx")
        if (s == null)
            throw new RuntimeException("db index cannot be empty, use dbIdx request field to specify the db index")

        val dbIdx = s.toInt

        if (dbIdx < 0 || dbIdx >= masterDbConfig.connStrs.size)
            throw new RuntimeException("db index is out of range dbIdx=%d".format(dbIdx))

        dbIdx
    }

    def generateSql(msqlSql: MsgSql, req: Request, splitTableType: Int): String = {

        var sql = msqlSql.sql

        if (msqlSql.tableIdx != null) {

            var v: String = null
            if (splitTableType == MsgDefine.SPLITTABLE_CUSTOM && masterDbConfig.splitTablePlugin != null) {
                v = masterDbConfig.splitTablePlugin.generateTableIdx(req)
            } else {
                v = generateTableIdx(splitTableType, msqlSql.tableIdxField, req)
            }
            if (v == null) {
                throw new RuntimeException("table index cannot be null")
            }
            sql = sql.replace("$" + msqlSql.tableIdx, v)

        }
        if (msqlSql.overwriteBuff != null) {
            var i = 0
            while (i < msqlSql.overwriteBuff.size) {

                val rdo = msqlSql.overwriteBuff(i)

                val v = req.s(rdo.key, rdo.defaultValue)
                if (v == null) {
                    throw new RuntimeException("replace field cannot be null, field=$" + rdo.idx)
                }

                // check injection
                if (hasInjection(v)) {
                    throw new RuntimeException("replace field has injection, field=$" + rdo.idx + ", v=" + v)
                }

                sql = sql.replace("$" + rdo.idx, v)

                i += 1
            }

        }

        sql
    }

    def hasInjection(v: String): Boolean = {
        val lv = v.toLowerCase
        for (k <- attackKeys) {
            if (lv.indexOf(k) >= 0) return true
        }

        false
    }

    def replaceSql(msqlSql: MsgSql, req: Request, splitTableType: Int): Tuple2[String, ArrayBufferString] = {

        val sql = generateSql(msqlSql, req, splitTableType)

        val values = new ArrayBufferString()

        for (keydef <- msqlSql.keys) {
            val value = req.s(keydef.key, keydef.defaultValue)
            values += value
        }

        (sql, values)
    }

    def replaceSqlBatch(msqlSql: MsgSql, req: Request, splitTableType: Int): Tuple2[String, ArrayBuffer[ArrayBufferString]] = {

        val sql = generateSql(msqlSql, req, splitTableType)

        val batchparams = new ArrayBuffer[ArrayBufferString]()

        val buff_any = new ArrayBuffer[Any]()
        val buff_size = new ArrayBuffer[Int]()

        for (keydef <- msqlSql.keys) {
            val value = req.body.getOrElse(keydef.key, null)
            buff_any += value
            if (value == null) buff_size += 1
            else {
                value match {
                    case s: String            => buff_size += 1
                    case i: Int               => buff_size += 1
                    case i: Long               => buff_size += 1
                    case i: Double               => buff_size += 1
                    case a: ArrayBufferString => buff_size += a.size
                    case a: ArrayBufferInt    => buff_size += a.size
                    case a: ArrayBufferLong    => buff_size += a.size
                    case a: ArrayBufferDouble    => buff_size += a.size
                    case _ =>
                        throw new RuntimeException("not supported type in request parameter")
                }
            }
        }

        var maxRows = 1
        var i = 0
        while (i < buff_size.size) {
            if (buff_size(i) > 1) {
                if (buff_size(i) > maxRows) {
                    if (maxRows == 1) maxRows = buff_size(i)
                    else throw new RuntimeException("batch update parameter size not match")
                }
            }
            i += 1
        }

        var kk = 0

        while (kk < maxRows) {
            val values = new ArrayBufferString()

            var mm = 0
            while (mm < buff_any.size) {

                val v = buff_any(mm)
                if (v == null) {

                    values += msqlSql.keys(mm).defaultValue

                } else {

                    v match {
                        case s: String            => values += s
                        case i: Int               => values += i.toString
                        case l: Long               => values += l.toString
                        case d: Double               => values += d.toString
                        case a: ArrayBufferString => values += a(kk)
                        case a: ArrayBufferInt    => values += a(kk).toString
                        case a: ArrayBufferLong    => values += a(kk).toString
                        case a: ArrayBufferDouble    => values += a(kk).toString
                    }

                }

                mm += 1
            }

            kk += 1
            batchparams += values
        }

        (sql, batchparams)
    }

    def replaceSqlBatchByRow(msqlSql: MsgSql, req: Request, splitTableType: Int): Tuple2[String, ArrayBuffer[ArrayBufferString]] = {

        val sql = generateSql(msqlSql, req, splitTableType)

        val batchparams = new ArrayBuffer[ArrayBufferString]()

        var dataMap: ArrayBufferMap = null
        var maxRows = 0
        for ((k, v) <- req.body) {
            if (v.isInstanceOf[ArrayBufferMap]) {
                dataMap = v.asInstanceOf[ArrayBufferMap]
                maxRows = dataMap.size
            }
        }
        if (maxRows == 0)
            throw new RuntimeException("no rows found in request parameter")

        var kk = 0

        while (kk < maxRows) {
            val values = new ArrayBufferString()

            for (keydef <- msqlSql.keys) {

                var v: Any = null
                if (keydef.keyInMap != null) {
                    v = dataMap(kk).getOrElse(keydef.keyInMap, keydef.defaultValueInMap)
                } else {
                    v = req.body.getOrElse(keydef.key, keydef.defaultValue)
                }
                v match {
                    case null      => values += null
                    case s: String => values += s
                    case i: Int    => values += i.toString
                }
            }

            kk += 1
            batchparams += values
        }

        (sql, batchparams)
    }

    def process(req: Request): Unit = {

        val msgDefine = msgMap.getOrElse(req.serviceId + "-" + req.msgId, null)
        if (msgDefine == null) {
            reply(req, ResultCodes.DB_ERROR)
            return
        }

        if (mode == DbLike.MODE_SYNC) {

            try {
                req.msgId match {

                    case DbLike.BEGINTRANSACTION =>
                        beginTransaction(masterList(0))
                        reply(req, 0)
                        return
                    case DbLike.COMMIT =>
                        commit()
                        reply(req, 0)
                        return
                    case DbLike.ROLLBACK =>
                        rollback()
                        reply(req, 0)
                        return
                    case _ =>
                        checkBindConnection()

                }

            } catch {
                case e: Throwable =>
                    log.error("db exception, e=%s".format(e.getMessage))
                    val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                    val body = new HashMapStringAny()
                    for (resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_SQLCODE) {
                        body.put(resdef.key, sqlCode)
                    }
                    val errorCode = ErrorCodeUtils.parseErrorCode(masterList(0), e)
                    reply(req, errorCode, body)
                    return
            }

        }

        val dbIdx = generateDbIdx(req)

        var results: DbResults = null

        val splitTableType =
            if (msgDefine.splitTableType != MsgDefine.SPLITTABLE_NODEFINE)
                msgDefine.splitTableType
            else
                masterDbConfig.splitTableType

        msgDefine.sqlType match {

            case MsgDefine.SQLTYPE_SELECT =>

                val (sql, params) = replaceSql(msgDefine.sqls(0), req, splitTableType)
                val keyTypes = msgDefine.sqls(0).keyTypes
                var useSlave = msgDefine.useSlave
                if (req.s("useSlave", "") != "") { useSlave = TypeSafe.isTrue(req.s("useSlave")) }
                val saveToFile = req.ns("saveToFile")
                val saveToFileSplitter = req.ns("saveToFileSplitter", ",")
                if (saveToFile == "")
                    results = query_db(sql, params, keyTypes, masterList, slaveList, dbIdx, useSlave)
                else
                    results = query_db_to_file(sql, params, keyTypes, masterList, slaveList, dbIdx, useSlave, saveToFile, saveToFileSplitter)

            case MsgDefine.SQLTYPE_UPDATE =>

                val (sql, params) = replaceSql(msgDefine.sqls(0), req, splitTableType)
                val keyTypes = msgDefine.sqls(0).keyTypes

                var insert_id_flag = false
                for (resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_ID || resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_IDS) { insert_id_flag = true }
                results = update_db(sql, params, keyTypes, masterList, dbIdx, insert_id_flag)

            case MsgDefine.SQLTYPE_MULTIUPDATE =>

                val sql_buff = new ArrayBuffer[String]()
                val params_buff = new ArrayBuffer[ArrayBufferString]()
                val keytypes_buff = new ArrayBuffer[ArrayBuffer[Int]]()

                for (msgSql <- msgDefine.sqls) {
                    val (sql, params) = replaceSql(msgSql, req, splitTableType)
                    sql_buff += sql
                    params_buff += params
                    keytypes_buff += msgSql.keyTypes
                }

                var insert_id_flag = false
                for (resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_ID || resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_IDS) { insert_id_flag = true }
                results = update_db_multi(sql_buff, params_buff, keytypes_buff, masterList, dbIdx, insert_id_flag)

            case MsgDefine.SQLTYPE_BATCHUPDATEBYCOL =>

                val (sql, batchparams) = replaceSqlBatch(msgDefine.sqls(0), req, splitTableType)
                val keyTypes = msgDefine.sqls(0).keyTypes

                var insert_id_flag = false
                for (resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_ID || resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_IDS) { insert_id_flag = true }
                results = update_db_batch(sql, batchparams, keyTypes, masterList, dbIdx, insert_id_flag)

            case MsgDefine.SQLTYPE_BATCHUPDATEBYROW =>

                val (sql, batchparams) = replaceSqlBatchByRow(msgDefine.sqls(0), req, splitTableType)
                val keyTypes = msgDefine.sqls(0).keyTypes

                var insert_id_flag = false
                for (resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_ID || resdef.fieldType == MsgDefine.RESULTTYPE_INSERT_IDS) { insert_id_flag = true }
                results = update_db_batch(sql, batchparams, keyTypes, masterList, dbIdx, insert_id_flag)

            case _ =>
                val body = new HashMapStringAny()
                reply(req, ResultCodes.DB_ERROR, body)
                return
        }

        if (results.rowCount == -1) {

            val body = new HashMapStringAny()
            for (resdef <- msgDefine.resdefs if resdef.fieldType == MsgDefine.RESULTTYPE_SQLCODE) {
                body.put(resdef.key, results.sqlCode)
            }

            val errorCode = ErrorCodeUtils.sqlCodeToErrorCode(masterList(0), results.sqlCode)
            reply(req, errorCode, body)
            return
        }

        val body = new HashMapStringAny()
        for (resdef <- msgDefine.resdefs) {

            resdef.fieldType match {

                case MsgDefine.RESULTTYPE_ROWCOUNT =>

                    body.put(resdef.key, results.rowCount)

                case MsgDefine.RESULTTYPE_INSERT_ID =>

                    if (results.insert_ids != null && results.insert_ids.length > 0)
                        body.put(resdef.key, results.insert_ids(0))

                case MsgDefine.RESULTTYPE_INSERT_IDS =>

                    if (results.insert_ids != null)
                        body.put(resdef.key, results.insert_ids)

                case MsgDefine.RESULTTYPE_COLUMN =>

                    body.put(resdef.key, results.value(resdef.row, resdef.col))

                case MsgDefine.RESULTTYPE_COLUMNLIST =>

                    body.put(resdef.key, results.valueList(resdef.col))

                case MsgDefine.RESULTTYPE_ROW =>

                    body.put(resdef.key, results.value(resdef.row, resdef.rowNames))

                case MsgDefine.RESULTTYPE_ALL =>

                    body.put(resdef.key, results.all(resdef.rowNames))

                case MsgDefine.RESULTTYPE_SQLCODE =>

                    body.put(resdef.key, 0)

                case _ =>

                    ;
            }
        }

        reply(req, 0, body)
    }

    def reply(req: Request, code: Int): Unit = {
        reply(req, code, new HashMapStringAny())
    }

    def reply(req: Request, code: Int, params: HashMapStringAny): Unit = {

        val res = new Response(code, params, req)
        dbActor.reply(new RequestResponseInfo(req, res))
    }

}

