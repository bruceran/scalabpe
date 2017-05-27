package scalabpe.plugin.neo4j

import org.neo4j.driver.v1._
import org.neo4j.driver.v1.summary._
import org.neo4j.driver.v1.exceptions._
import org.neo4j.driver.internal.value._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.io._

import scalabpe.core._

class LocalFile(val filename:String) { 

    var writer:PrintWriter = null
 
    open()

    def open() {
        try {
            writer = new PrintWriter( new OutputStreamWriter(new FileOutputStream(filename,false),"utf-8") );
        }
        catch {
            case e:Throwable =>
                writer = null
        }
    }

    def close() {
        if( writer != null ) {
            writer.close()
            writer = null
        }
    }
    
    def writeLine(line:String):Unit = {
        if( writer == null ) return 
        try {
            writer.println(line)
        }
        catch {
            case e:Throwable =>
                try { if( writer != null ) writer.close(); } catch { case e:Throwable => }
                writer = null
        }
    }
}
trait QueryCallback {
    def process(rowNum:Int, buff:ArrayBufferString):Boolean
}

object ColumnType {

    val COLUMNTYPE_STRING = 1
    val COLUMNTYPE_NUMBER = 2

    def toColumnType(s:String):Int = {
        s.toLowerCase() match {
            case "string" => COLUMNTYPE_STRING
            case "number" => COLUMNTYPE_NUMBER
            case _ => COLUMNTYPE_STRING
        }
    }

}

object DbResults {
    val emptyResults = ArrayBuffer[ ArrayBufferAny ]()
}

class DbResults (
    val rowCount:Int,
    val results:ArrayBuffer[ ArrayBufferAny ],
    val sqlCode:String = "0" ) {

    def this(rowCount:Int) {
        this(rowCount,DbResults.emptyResults,"0")
    }
    def this(rowCount:Int,sqlCode:String) {
        this(rowCount,DbResults.emptyResults,sqlCode)
    }

    def all(rownames:List[String]) : ArrayBufferMap = {

        val ss = new ArrayBufferMap()
        var i = 0
        while(i < rowCount) {
            ss += value(i,rownames)
            i+=1
        }
        ss
    }

    def value(row:Int,col:Int):Any ={

        if( row<0||row>=results.size)
            return null

        val data = results(row)

        if( col<0||col>=data.size)
            return null

        data(col)
    }

    def value(row:Int,rownames:List[String]):HashMapStringAny = {
        if( row<0||row>=results.size)
            return null
        val data = results(row)
        val returnmap = new HashMapStringAny()
        var i = 0
        for(rowname <- rownames ) {
            if( i < data.size) {
                returnmap.put(rowname,data(i))
            }
            i+=1
        }
        returnmap
    }

    def valueList(col:Int):ArrayBufferAny = {

        if( results.size == 0 ) return null

        val firstrow = results(0)
        if( col<0||col>=firstrow.size)
            return null

        val returnarray = new ArrayBufferAny()

        for( row <- results ) {
            returnarray += row(col)
        }

        returnarray
    }

    override def toString() = {
        "rowCount=%d,results=%s".format(rowCount,results.mkString(","))
    }
}

class DbConnStr(val jdbcString:String, val username:String, val password:String)

object DbLike extends Logging {
    val MODE_ASYNC = 1
    val MODE_SYNC = 2

    val BEGINTRANSACTION = 10000
    val COMMIT = 10001
    val ROLLBACK = 10002

    val regDb1 = """^service=([^ ]+)[ ]+user=([^ ]*)[ ]+password=([^ ]*)$""".r

    loadSo

    def loadSo() {

        try {
            System.loadLibrary("sec");
            log.info("library sec loaded")
        } catch {
            case e:Throwable =>
                log.error("cannot load library sec")
        }

    }
}

class DbLike extends Logging  {

    import scalabpe.plugin.neo4j.DbLike._

    var mode = DbLike.MODE_ASYNC
    var longTimeSql = 500
    val tl = new ThreadLocal[Session]()
    val txtl = new ThreadLocal[Transaction]()

    def isTransactionMsgId(msgId:Int) : Boolean = {
        msgId == DbLike.BEGINTRANSACTION || msgId == DbLike.COMMIT ||  msgId == DbLike.ROLLBACK
    }

    def decrypt(pwd:String):String = {
        if (pwd.startsWith("des:")) {
            decryptDes(pwd.substring(4))
        } else if (pwd.startsWith("desx:")) {
            decryptDesX(pwd.substring(5))
        } else if (pwd.startsWith("rsa:")) {
            decryptRsa(pwd.substring(4))
        } else {
            pwd
        }

    }

    @native def decryptDes(s:String) : String;
    @native def decryptDesX(s:String) : String;
    @native def decryptRsa(s:String) : String;

    def parseDbConnStr(s:String): DbConnStr = {
        s match {
            case regDb1(jdbcString,username,password) =>
                return new DbConnStr(jdbcString,username,password)
            case _ =>
                throw new RuntimeException("connection string is not valid,conn=%s".format(s))
        }
    }

    def createDriver(jdbcString:String,username:String,password:String,connNum:Int = 1): Driver = {
        val driver = GraphDatabase.driver( jdbcString,AuthTokens.basic(username,password), Config.build().withMaxSessions( connNum ).toConfig() )
        driver
    }

    def hasError(ds: Driver) : Boolean = {

        try {

            val result = query_db("Match (n:Dual) return n.id limit 1",ArrayBufferString(),ArrayBufferString(),ArrayBufferInt(),ds)
            return result.rowCount == -1

        } catch {
            case e:Throwable =>
                return true
        }

    }

    def dump(ds: Driver): Unit = {
        val buff = new StringBuilder
        log.info(buff.toString)
    }

    def closeDriver(ds: Driver): Unit = {
        try {
            ds.close()
        } catch {
            case e:Throwable =>
        }
    }

    def prepare(params:ArrayBufferString,  paramNames:ArrayBufferString, paramTypes:ArrayBuffer[Int] ):java.util.Map[String,Object] = {

        val map = new java.util.HashMap[String,Object]()

        if( params == null || params.size == 0 ) return map

        var i = 0
        while(i < params.size) {

            paramTypes(i) match {

                case ColumnType.COLUMNTYPE_NUMBER =>

                    if( params(i) == null ) {
                        map.put(paramNames(i),null)
                    } else {
                        map.put(paramNames(i),new java.lang.Integer(java.lang.Integer.parseInt(params(i))))
                    }

                case _ =>

                    map.put(paramNames(i),params(i))
            }

            i+=1
        }

        map
    }

    def query_db(sql:String,params:ArrayBufferString,paramNames:ArrayBufferString,paramTypes:ArrayBuffer[Int],ds:Driver ):DbResults = {

        var conn : Session = null

        val ts1 = System.currentTimeMillis

        if( log.isDebugEnabled() ) {
            var paramsstr = ""
            if( params != null ) paramsstr = params.mkString(",")
            log.debug("db query, sql=%s,value=%s".format(sql,paramsstr))
        }

        val results = new ArrayBuffer[ ArrayBufferAny ] ()

        var hasException = false
        try {

            if( mode == DbLike.MODE_SYNC ) {
                conn = tl.get()
            }else {
                conn = ds.session()
            }

            val rs = conn.run(sql,prepare(params,paramNames,paramTypes))

            val columnCount = rs.keys().size()

            while( rs.hasNext()) {

                val record = rs.next()

                val row = new ArrayBufferAny()
                var i=0
                while(i < columnCount) {
                    var value = record.get(i)
                    if( value == null )
                        row += null
                    else {
                        row += valueToString(value)
                    }
                    i+=1
                }

                results += row
            }


            val ret = new DbResults(results.size,results)
            ret

        } catch {
            case e:Throwable =>
                hasException = true
                var paramsstr = ""
                if( params != null ) paramsstr = params.mkString(",")
                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db query exception, e=%s,sql=%s,value=%s,sqlCode=%s".format(e.getMessage,sql,paramsstr,sqlCode))
                new DbResults(-1,sqlCode)
        } finally {

            val ts = System.currentTimeMillis - ts1
            if( ts >= longTimeSql ) {
                log.warn("long time sql, sql="+sql+", ts=["+ts+"]ms")
            }

            if( hasException ) {

                if( mode == DbLike.MODE_SYNC ) {
                    mustRollback()
                }

            }
            if( mode != DbLike.MODE_SYNC ) {
                closeConnection(conn)
                conn = null
            }
        }

    }
    
    def query_db_to_file(sql:String,params:ArrayBufferString,paramNames:ArrayBufferString,paramTypes:ArrayBuffer[Int],ds:Driver,saveToFile:String,splitter:String):DbResults = {

        var conn : Session = null

        val ts1 = System.currentTimeMillis

        if( log.isDebugEnabled() ) {
            var paramsstr = ""
            if( params != null ) paramsstr = params.mkString(",")
            log.debug("db query, sql=%s,value=%s".format(sql,paramsstr))
        }

        val localFile = new LocalFile(saveToFile)
        var totalRowCount = 0

        var hasException = false
        try {

            if( mode == DbLike.MODE_SYNC ) {
                conn = tl.get()
            }else {
                conn = ds.session()
            }

            val rs = conn.run(sql,prepare(params,paramNames,paramTypes))

            val columnCount = rs.keys().size()

            while( rs.hasNext()) {

                val record = rs.next()

                val row = new ArrayBufferAny()
                var i=0
                while(i < columnCount) {
                    var value = record.get(i)
                    if( value == null )
                        row += null
                    else {
                        row += valueToString(value)
                    }
                    i+=1
                }

                totalRowCount += 1
                val line = row.mkString(splitter)
                localFile.writeLine(line)
            }

            val results = new ArrayBuffer[ ArrayBufferAny ] ()
            val ret = new DbResults(totalRowCount,results)
            ret

        } catch {
            case e:Throwable =>
                hasException = true
                var paramsstr = ""
                if( params != null ) paramsstr = params.mkString(",")
                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db query exception, e=%s,sql=%s,value=%s,sqlCode=%s".format(e.getMessage,sql,paramsstr,sqlCode))
                new DbResults(-1,sqlCode)
        } finally {

            if( localFile != null ) localFile.close()

            val ts = System.currentTimeMillis - ts1
            if( ts >= longTimeSql ) {
                log.warn("long time sql, sql="+sql+", ts=["+ts+"]ms")
            }

            if( hasException ) {

                if( mode == DbLike.MODE_SYNC ) {
                    mustRollback()
                }

            }
            if( mode != DbLike.MODE_SYNC ) {
                closeConnection(conn)
                conn = null
            }
        }

    }
    
    def query_db(sql:String,ds:Driver,callback:QueryCallback):Int = {

        var conn : Session = null

        val ts1 = System.currentTimeMillis

        if( log.isDebugEnabled() ) {
            log.debug("db query, sql=%s".format(sql))
        }

        var totalRowCount = 0
        var hasException = false
        try {

            conn = ds.session()

            val rs = conn.run(sql)

            val columnCount = rs.keys().size()

            while( rs.hasNext()) {

                val record = rs.next()

                val row = new ArrayBufferString()
                var i=0
                while(i < columnCount) {
                    var value = record.get(i)
                    if( value == null )
                        row += null
                    else {
                        row += valueToString(value)
                    }
                    i+=1
                }
                totalRowCount += 1

                val continue = callback.process(totalRowCount,row)
                if( !continue ) {
                    return totalRowCount
                }
            }

            totalRowCount

        } catch {
            case e:Throwable =>
                hasException = true
                log.error("db query exception, e=%s,sql=%s".format(e.getMessage,sql))
                -1
        } finally {

            val ts = System.currentTimeMillis - ts1
            if( ts >= longTimeSql ) {
                log.warn("long time sql, sql="+sql+", ts=["+ts+"]ms")
            }

            closeConnection(conn)
            conn = null
        }

    }
    
    def valueToString(o:Object):String = {
        if( !o.isInstanceOf[Value] ) return o.toString
        val v = o.asInstanceOf[Value]
        v match {
            case n:NullValue => null
            case l:ListValue => 
                val list = l.asList
                val buff = new ArrayBufferString()
                val it = list.iterator()
                while( it.hasNext() ) {
                    val e = it.next()
                    buff += String.valueOf(e)
                }
                JsonCodec.mkString(buff)
            case m:MapValue => 
                val map = m.asMap
                val m2 = new HashMapStringAny()
                val it = map.entrySet().iterator()
                while( it.hasNext() ) {
                    val e = it.next()
                    m2.put(e.getKey(),valueToString(e.getValue()))
                }
                JsonCodec.mkString(m2)

            case n:NodeValue => 
                val map = n.asNode.asMap
                val m2 = new HashMapStringAny()
                val it = map.entrySet().iterator()
                while( it.hasNext() ) {
                    val e = it.next()
                    m2.put(e.getKey(),valueToString(e.getValue()))
                }
                m2.put("_id",n.asNode.id())
                JsonCodec.mkString(m2)

            case n:RelationshipValue => 
                val map = n.asRelationship().asMap
                val m2 = new HashMapStringAny()
                val it = map.entrySet().iterator()
                while( it.hasNext() ) {
                    val e = it.next()
                    m2.put(e.getKey(),valueToString(e.getValue()))
                }
                m2.put("_id",n.asRelationship.id())
                JsonCodec.mkString(m2)

            case i:IntegerValue => String.valueOf(i.asLong)
            case f:FloatValue => String.valueOf(f.asDouble)
            case b:BooleanValue => String.valueOf(b.asBoolean)
            case s:StringValue => s.asString
            case _ => v.toString
        }
    }

    def all(v:SummaryCounters):Int = {
        v.constraintsAdded() + v.constraintsRemoved() + v.indexesAdded() + v.indexesRemoved() + v.labelsAdded() + v.labelsRemoved() + v.nodesCreated() + v.nodesDeleted() + v.propertiesSet() + v.relationshipsCreated() + v.relationshipsDeleted() 
    }

    def update_db(sql:String,params:ArrayBufferString,paramNames:ArrayBufferString,paramTypes:ArrayBuffer[Int],ds:Driver):DbResults = {

        var conn : Session = null

        if( log.isDebugEnabled() ) {
            var paramsstr = ""
            if( params != null ) paramsstr = params.mkString(",")

            log.debug("db update, sql=%s,value=%s".format(sql,paramsstr))
        }

        val ts1 = System.currentTimeMillis

        var hasException = false

        try {

            if( mode == DbLike.MODE_SYNC ) {
                conn = tl.get()
            }else {
                conn = ds.session()
            }

            val rs = conn.run(sql,prepare(params,paramNames,paramTypes))
            val summary = rs.consume()
            val counters = summary.counters()
            val cnt = all(counters)
            val ret = new DbResults(cnt)

            ret

        } catch {
            case e:Throwable =>
                hasException = true
                var paramsstr = ""
                if( params != null ) paramsstr = params.mkString(",")

                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db update exception, e=%s,sql=%s,value=%s,sqlCode=%s".format(e.getMessage,sql,paramsstr,sqlCode))
                new DbResults(-1,sqlCode)
        } finally {

            val ts = System.currentTimeMillis - ts1
            if( ts >= longTimeSql ) {
                log.warn("long time sql, sql="+sql+", ts=["+ts+"]ms")
            }

            if( hasException ) {

                if( mode == DbLike.MODE_SYNC ) {
                    mustRollback()
                }

            }
            if( mode != DbLike.MODE_SYNC ) {
                closeConnection(conn)
                conn = null
            }
        }
    }

    def update_db_multi(sqls_buff:ArrayBuffer[String],params_buff:ArrayBuffer[ArrayBufferString],
        paramNames_buff:ArrayBuffer[ArrayBufferString],keytypes_buff:ArrayBuffer[ArrayBuffer[Int]],
        ds:Driver):DbResults = {

            var conn : Session = null
            var tx : Transaction = null

            if( log.isDebugEnabled() ) {

                for(i <- 0 until sqls_buff.size ) {
                    val sql = sqls_buff(i)
                    val params = params_buff(i)
                    var paramsstr = ""
                    if( params != null ) paramsstr = params.mkString(",")

                    log.debug("db update multi #%d, sql=%s,value=%s".format(i+1,sql,paramsstr))
                }

            }
            val ts1 = System.currentTimeMillis

            var hasException = false

            var totalrows = 0

            try {

                if( mode == DbLike.MODE_SYNC ) {
                    conn = tl.get()
                    tx = txtl.get()
                }else {
                    conn = ds.session()
                    tx = conn.beginTransaction()
                }

                var sqlii = 0

                while( sqlii < sqls_buff.size ) {

                    val params = params_buff(sqlii)
                    val paramNames = paramNames_buff(sqlii)
                    val keytypes = keytypes_buff(sqlii)
                    val map = prepare(params,paramNames,keytypes)
                    val rs = tx.run(sqls_buff(sqlii),map)
                    val summary = rs.consume()
                    val counters = summary.counters()
                    val cnt = all(counters)

                    totalrows += cnt
                    sqlii += 1
                }

                val results = new DbResults(totalrows)

                if( mode != DbLike.MODE_SYNC ) {
                    tx.success()
                    tx.close()
                }

                results

            } catch {
                case e:Throwable => {
                    hasException = true

                    var paramsstr = ""
                    if( params_buff != null ) paramsstr = params_buff.mkString("^_^")

                    val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                    log.error("db update multi exception, e=%s,sqls=%s,values=%s,sqlcode=%s".format(e.getMessage,sqls_buff.mkString("^_^"),paramsstr,sqlCode))
                    val results = new DbResults(-1,sqlCode)
                    results
                }
            } finally {

                val ts = System.currentTimeMillis - ts1
                if( ts >= longTimeSql ) {
                    log.warn("long time sql, sqls="+sqls_buff.mkString("^_^")+", ts=["+ts+"]ms")
                }

                if( hasException ) {

                    if( mode == DbLike.MODE_SYNC ) {
                        mustRollback()
                    } else {
                        try { tx.failure(); tx.close(); } catch {case e:Throwable => log.error("rollback error") };
                    }

                }

                if( mode != DbLike.MODE_SYNC ) {
                    closeConnection(conn)
                    conn = null
                }
            }

    }

    def update_db_batch(sql:String,params_buff:ArrayBuffer[ArrayBufferString],
        paramNames:ArrayBufferString,keytypes: ArrayBuffer[Int] ,
        ds:Driver):DbResults = {

            var conn : Session = null
            var tx : Transaction = null

            if( log.isDebugEnabled() ) {
                log.debug("db update batch, sql=%s".format(sql))
            }
            val ts1 = System.currentTimeMillis

            var hasException = false

            try {

                if( mode == DbLike.MODE_SYNC ) {
                    conn = tl.get()
                    tx = txtl.get()
                }else {
                    conn = ds.session()
                    tx = conn.beginTransaction()
                }

                var totalrows = 0
                var i = 0
                while( i < params_buff.size ) {
                    val params = params_buff(i)
                    val map = prepare(params,paramNames,keytypes)
                    val rs = tx.run(sql,map)
                    val summary = rs.consume()
                    val counters = summary.counters()
                    val cnt = all(counters)

                    totalrows += cnt
                    i += 1
                }

                val results = new DbResults(totalrows)

                if( mode != DbLike.MODE_SYNC ) {
                    tx.success()
                    tx.close()
                }

                results

            } catch {
                case e:Throwable => {
                    hasException = true
                    val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                    log.error("db update batch exception, e=%s,sql=%s,sqlCode=%s".format(e.getMessage,sql,sqlCode))
                    val results = new DbResults(-1,sqlCode)
                    results
                }
            } finally {

                val ts = System.currentTimeMillis - ts1
                if( ts >= longTimeSql ) {
                    log.warn("long time sql, sql="+sql+", ts=["+ts+"]ms")
                }

                if( hasException ) {

                    if( mode == DbLike.MODE_SYNC ) {
                        mustRollback()
                    } else {
                        try { tx.failure(); tx.close(); } catch {case e:Throwable => log.error("rollback error") };
                    }

                }

                if( mode != DbLike.MODE_SYNC ) {
                    closeConnection(conn)
                    conn = null
                }
            }

    }

    def checkBindConnection() {
        val conn = tl.get()
        if( conn == null )
            throw new RuntimeException("beginTransaction not called")
    }

    def beginTransaction(ds:Driver) {

        var conn = tl.get()
        if( conn != null )
            throw new RuntimeException("beginTransaction already called")

        conn = ds.session()
        val tx = conn.beginTransaction()
        tl.set(conn)
        txtl.set(tx)
    }

    def commit() {

        var conn = tl.get()
        var tx = txtl.get()
        if( conn == null )
            throw new RuntimeException("beginTransaction not called")

        try {
            tl.remove()
            txtl.remove()
            tx.success()
            tx.close()
        } catch {
            case e:Throwable =>
                mustRollback()
                throw e
        } finally {
            closeConnection(conn)
            conn = null
        }

    }

    def rollback() {

        var conn = tl.get()
        var tx = txtl.get()
        if( conn == null ) return // allow rollback to be called more than one time

        try {
            tl.remove()
            txtl.remove()
            tx.failure()
            tx.close()
        } finally {
            closeConnection(conn)
            conn = null
        }

    }

    def mustRollback() {
        try {
            rollback()
        } catch {
            case e:Throwable =>
                log.error("rollback error")
        }
    }

    def closeConnection(conn:Session) {
        if( conn != null ) {
            try {
                conn.close();
            } catch {
                case e:Throwable =>
                    log.error("close connection error",e)
            };
        }
    }

}

object ErrorCodeUtils extends Logging {

    val connReg = """^.*Unable to connect to.*$""".r

    val unknown_sql_errorcode  = "99999997"
    val timeout_errorcode      = "99999998"
    val connect_errorcode      = "99999999"

    def mergeLines(ss: String): String = {
        var sss = ss.replace("\n\r"," ").replace("\r"," ").replace("\n"," ")
        sss
    }

    def parseErrorCode(e: Throwable): Int = {
        val sqlCode = parseSqlCode(e)
        sqlCodeToErrorCode(sqlCode)
    }

    def parseSqlCode(e: Throwable): String = {

        val sqle = fetchException(e)
        if( sqle == null ) return unknown_sql_errorcode
        
        val sqlcode = sqle.neo4jErrorCode()
        if( sqlcode != null && sqlcode != "N/A" ) return sqlcode

        if( sqle.getMessage == null ) return unknown_sql_errorcode

        val ss = mergeLines(sqle.getMessage)
        if( log.isDebugEnabled) {
            log.debug("sql e:"+ss)
        }

        ss match {
            case connReg() =>
                return connect_errorcode
            case _ =>
                return unknown_sql_errorcode
        }
    }

    def sqlCodeToErrorCode(sqlCode:String): Int = {
        if( sqlCode.startsWith("Neo.ClientError.Security." ))  return ResultCodes.DB_CONN_FAILED
        if( sqlCode == "Neo.TransientError.Network.CommunicationError" ) return ResultCodes.DB_CONN_FAILED
        if( sqlCode == "Neo.TransientError.General.DatabaseUnavailable" ) return ResultCodes.DB_CONN_FAILED
        if( sqlCode == "Neo.TransientError.General.OutOfMemoryError" ) return ResultCodes.DB_CONN_FAILED
        if( sqlCode == "Neo.TransientError.General.StackOverFlowError" ) return ResultCodes.DB_CONN_FAILED
        if( sqlCode == connect_errorcode ) return ResultCodes.DB_CONN_FAILED
        if( sqlCode == timeout_errorcode ) return ResultCodes.DB_TIMEOUT
        return ResultCodes.DB_ERROR
    }

    def fetchException(se:Throwable): Neo4jException = {

        var e = se
        var t : Throwable = null
        var needBreak = false

        while(!needBreak) {
            t = e.getCause()
            if( t == null ) needBreak = true
            else if( t == e ) needBreak = true
            else e = t
        }

        if( e.isInstanceOf[Neo4jException]) {
            val sqle = e.asInstanceOf[Neo4jException]
            return sqle
        }

        if( e.getMessage() != null ) {
            return new ClientException(e.getMessage())
        }

        return null
    }

}

