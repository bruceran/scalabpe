package scalabpe.plugin

import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.sql.Types

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.dbcp.BasicDataSource

import javax.sql.DataSource
import scalabpe.core.ArrayBufferAny
import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.ResultCodes

class LocalFile(val filename: String) {

    var writer: PrintWriter = null

    open()

    def open() {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filename, false), "utf-8"));
        } catch {
            case e: Throwable =>
                writer = null
        }
    }

    def close() {
        if (writer != null) {
            writer.close()
            writer = null
        }
    }

    def writeLine(line: String): Unit = {
        if (writer == null) return
        try {
            writer.println(line)
        } catch {
            case e: Throwable =>
                try { if (writer != null) writer.close(); } catch { case e: Throwable => }
                writer = null
        }
    }
}
object ColumnType {

    val COLUMNTYPE_STRING = 1
    val COLUMNTYPE_NUMBER = 2
    val COLUMNTYPE_DATE = 3
    val COLUMNTYPE_DATETIME = 4

    def toColumnType(s: String): Int = {
        s.toLowerCase() match {
            case "string"    => COLUMNTYPE_STRING
            case "number"    => COLUMNTYPE_NUMBER
            case "date"      => COLUMNTYPE_DATE
            case "datetime"  => COLUMNTYPE_DATETIME
            case "timestamp" => COLUMNTYPE_DATETIME
            case _           => COLUMNTYPE_STRING
        }
    }

}

object DbResults {
    val emptyResults = ArrayBuffer[ArrayBufferAny]()
}

class DbResults(
        val rowCount: Int,
        val results: ArrayBuffer[ArrayBufferAny],
        val sqlCode: Int = 0) {

    var insert_ids: ArrayBufferString = null

    def this(rowCount: Int) {
        this(rowCount, DbResults.emptyResults, 0)
    }
    def this(rowCount: Int, sqlCode: Int) {
        this(rowCount, DbResults.emptyResults, sqlCode)
    }

    def all(rownames: List[String]): ArrayBufferMap = {

        val ss = new ArrayBufferMap()
        var i = 0
        while (i < rowCount) {
            ss += value(i, rownames)
            i += 1
        }
        ss
    }

    def value(row: Int, col: Int): Any = {

        if (row < 0 || row >= results.size)
            return null

        val data = results(row)

        if (col < 0 || col >= data.size)
            return null

        data(col)
    }

    def value(row: Int, rownames: List[String]): HashMapStringAny = {
        if (row < 0 || row >= results.size)
            return null
        val data = results(row)
        val returnmap = new HashMapStringAny()
        var i = 0
        for (rowname <- rownames) {
            if (i < data.size) {
                returnmap.put(rowname, data(i))
            }
            i += 1
        }
        returnmap
    }

    def valueList(col: Int): ArrayBufferAny = {

        if (results.size == 0) return null

        val firstrow = results(0)
        if (col < 0 || col >= firstrow.size)
            return null

        val returnarray = new ArrayBufferAny()

        for (row <- results) {
            returnarray += row(col)
        }

        returnarray
    }

    override def toString() = {
        "rowCount=%d,results=%s".format(rowCount, results.mkString(","))
    }
}

class DbConnStr(val jdbcString: String, val username: String, val password: String)

object DbLike extends Logging {
    val MODE_ASYNC = 1
    val MODE_SYNC = 2

    val BEGINTRANSACTION = 10000
    val COMMIT = 10001
    val ROLLBACK = 10002

    val regDb1 = """^service=([^ ]+)[ ]+user=([^ ]*)[ ]+password=([^ ]*)$""".r
    val regDb2 = """^([^ ]+)[ ]+user=([^ ]*)[ ]+password=([^ ]*)$""".r

    def getDbType(ds: DataSource): String = {
        val url = getUrl(ds)
        if (url.indexOf("oracle") >= 0) return "oracle"
        if (url.indexOf("mysql") >= 0) return "mysql"
        if (url.indexOf("sqlserver") >= 0) return "sqlserver"
        ""
    }

    def getUrl(ds: DataSource): String = {
        ds match {
            case t: BasicDataSource => t.getUrl()
            case _                  => ""
        }
    }

    loadSo

    def loadSo() {

        try {
            System.loadLibrary("sec");
            log.info("library sec loaded")
        } catch {
            case e: Throwable =>
                log.error("cannot load library sec")
        }

    }
}

class DbLike extends Logging {

    import scalabpe.plugin.DbLike._

    var mode = DbLike.MODE_ASYNC
    var longTimeSql = 500
    val tl = new ThreadLocal[Connection]()

    def isTransactionMsgId(msgId: Int): Boolean = {
        msgId == DbLike.BEGINTRANSACTION || msgId == DbLike.COMMIT || msgId == DbLike.ROLLBACK
    }

    def decrypt(pwd: String): String = {
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

    @native def decryptDes(s: String): String;
    @native def decryptDesX(s: String): String;
    @native def decryptRsa(s: String): String;

    def parseDbConnStr(s: String): DbConnStr = {
        s match {
            case regDb1(jdbcString, username, password) =>
                return new DbConnStr(jdbcString, username, password)
            case regDb2(jdbcString, username, password) =>
                return new DbConnStr(jdbcString, username, password)
            case _ =>
                throw new RuntimeException("connection string is not valid,conn=%s".format(s))
        }
    }

    def createDataSource(jdbcString: String, username: String, password: String, connNum: Int = 1): DataSource = {

        var clsName: String = null
        var isMysql: Boolean = false
        if (jdbcString.indexOf("oracle") >= 0) {
            clsName = "oracle.jdbc.driver.OracleDriver"
            isMysql = false
        } else if (jdbcString.indexOf("mysql") >= 0) {
            clsName = "com.mysql.jdbc.Driver"
            isMysql = true
        } else if (jdbcString.indexOf("jtds") >= 0) {
            clsName = "net.sourceforge.jtds.jdbc.Driver"
            isMysql = false
        } else if (jdbcString.indexOf("sqlserver") >= 0) {
            clsName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            isMysql = false
        }

        val ds = new BasicDataSource
        ds.setDriverClassName(clsName)
        ds.setUrl(jdbcString)
        ds.setUsername(decrypt(username))
        ds.setPassword(decrypt(password))
        ds.setMaxWait(8000) // milliseconds
        ds.setMaxActive(connNum)
        ds.setMaxIdle(connNum)
        ds.setMinIdle(connNum)
        ds.setInitialSize(connNum);

        if (isMysql) {
            ds.setValidationQuery("select now()")
            ds.setTestWhileIdle(true)
            ds.setTimeBetweenEvictionRunsMillis(300000)
            ds.setMinEvictableIdleTimeMillis(5 * 60 * 1000L)
            var testNum = connNum / 2
            if (testNum < 1) testNum = 1
            ds.setNumTestsPerEvictionRun(testNum)
        }

        ds

    }

    def hasError(ds: DataSource): Boolean = {

        try {

            val dbType = getDbType(ds)
            dbType match {
                case "oracle" =>
                    val result = query_db("select sysdate from dual", null, null, ds)
                    return result.rowCount == -1
                case "mysql" =>
                    val result = query_db("select now()", null, null, ds)
                    return result.rowCount == -1
                case "sqlserver" =>
                    val result = query_db("select getdate()", null, null, ds)
                    return result.rowCount == -1
                case _ => false
            }
            return false

        } catch {
            case e: Throwable =>
                return true
        }

    }

    def dump(ds: DataSource): Unit = {

        val buff = new StringBuilder

        ds match {

            case ds: BasicDataSource =>

                log.info("--- db url=" + ds.getUrl + ", username=" + ds.getUsername)
                buff.append("ds.getNumActive=").append(ds.getNumActive).append(",")
                buff.append("ds.getNumIdle=").append(ds.getNumIdle).append(",")

        }

        log.info(buff.toString)
    }

    def closeDataSource(ds: DataSource): Unit = {
        ds match {
            case t: BasicDataSource => t.close()
        }
    }

    def prepare(ps: PreparedStatement, params: ArrayBufferString, keyTypes: ArrayBuffer[Int]) {

        if (params == null || params.size == 0) return

        var fdt: java.text.SimpleDateFormat = null
        var fd: java.text.SimpleDateFormat = null

        var i = 0
        while (i < params.size) {

            keyTypes(i) match {

                case ColumnType.COLUMNTYPE_DATE =>

                    if (fd == null) fd = new java.text.SimpleDateFormat("yyyy-MM-dd")

                    if (params(i) == null) {
                        ps.setDate(i + 1, null)
                    } else {
                        val d = new java.sql.Date(fd.parse(params(i)).getTime())
                        ps.setDate(i + 1, d)
                    }

                case ColumnType.COLUMNTYPE_DATETIME =>

                    if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

                    if (params(i) == null) {
                        ps.setTimestamp(i + 1, null)
                    } else {
                        val d = new java.sql.Timestamp(fdt.parse(params(i)).getTime())
                        ps.setTimestamp(i + 1, d)
                    }

                case ColumnType.COLUMNTYPE_NUMBER =>

                    if (params(i) == null) {
                        ps.setString(i + 1, null)
                    } else {
                        ps.setInt(i + 1, java.lang.Integer.parseInt(params(i)))
                    }

                case _ =>

                    ps.setString(i + 1, params(i))
            }

            i += 1
        }
    }

    def query_db(sql: String, params: ArrayBufferString, keyTypes: ArrayBuffer[Int], masterList: ArrayBuffer[DataSource], slaveList: ArrayBuffer[DataSource], dbIdx: Int, useSlave: Boolean = false): DbResults = {
        if (useSlave && slaveList != null && slaveList.size > 0) {
            var ds = slaveList(dbIdx)

            val result = query_db(sql, params, keyTypes, ds)
            if (result.rowCount != -1)
                return result
        }

        var ds = masterList(dbIdx)
        val result = query_db(sql, params, keyTypes, ds)
        return result
    }

    def query_db(sql: String, params: ArrayBufferString, keyTypes: ArrayBuffer[Int], ds: DataSource): DbResults = {

        var conn: java.sql.Connection = null
        var ps: PreparedStatement = null
        var rs: ResultSet = null

        val ts1 = System.currentTimeMillis

        if (log.isDebugEnabled()) {
            var paramsstr = ""
            if (params != null) paramsstr = params.mkString(",")
            log.debug("db query, sql=%s,value=%s".format(sql, paramsstr))
        }

        val results = new ArrayBuffer[ArrayBufferAny]()

        var hasException = false
        try {

            if (mode == DbLike.MODE_SYNC) {
                conn = tl.get()
            } else {
                conn = ds.getConnection()
            }

            ps = conn.prepareStatement(sql)

            prepare(ps, params, keyTypes)

            rs = ps.executeQuery()

            val metaData = rs.getMetaData()
            val columnCount = metaData.getColumnCount()

            var fdt: java.text.SimpleDateFormat = null

            while (rs.next()) {
                val row = new ArrayBufferAny()
                var i = 1
                while (i <= columnCount) {
                    val colType = metaData.getColumnType(i)

                    var value: Any = null
                    var tmpValue: Any = null

                    colType match {
                        case Types.DATE =>
                            if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            tmpValue = rs.getTimestamp(i)
                            if (tmpValue != null) {
                                value = fdt.format(tmpValue.asInstanceOf[java.util.Date])
                            }
                        case Types.TIMESTAMP =>
                            if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            tmpValue = rs.getTimestamp(i)
                            if (tmpValue != null)
                                value = fdt.format(tmpValue.asInstanceOf[java.util.Date])
                        case _ =>
                            value = rs.getString(i)
                    }

                    row += value
                    i += 1
                }

                results += row
            }

            new DbResults(results.size, results)

        } catch {
            case e: Throwable =>
                hasException = true
                var paramsstr = ""
                if (params != null) paramsstr = params.mkString(",")
                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db query exception, e=%s,sql=%s,value=%s,sqlCode=%d".format(e.getMessage, sql, paramsstr, sqlCode))
                new DbResults(-1, sqlCode)
        } finally {
            closeResultSet(rs)
            rs = null
            closeStatement(ps)
            ps = null

            val ts = System.currentTimeMillis - ts1
            if (ts >= longTimeSql) {
                log.warn("long time sql, sql=" + sql + ", ts=[" + ts + "]ms")
            }

            if (hasException) {

                if (mode == DbLike.MODE_SYNC) {
                    mustRollback()
                }

            }
            if (mode != DbLike.MODE_SYNC) {
                closeConnection(conn)
                conn = null
            }
        }

    }

    def query_db_to_file(sql: String, params: ArrayBufferString, keyTypes: ArrayBuffer[Int], masterList: ArrayBuffer[DataSource], slaveList: ArrayBuffer[DataSource], dbIdx: Int, useSlave: Boolean = false, saveToFile: String, splitter: String): DbResults = {
        if (useSlave && slaveList != null && slaveList.size > 0) {
            var ds = slaveList(dbIdx)

            val result = query_db_to_file(sql, params, keyTypes, ds, saveToFile, splitter)
            if (result.rowCount != -1)
                return result
        }

        var ds = masterList(dbIdx)
        val result = query_db_to_file(sql, params, keyTypes, ds, saveToFile, splitter)
        return result
    }

    def query_db_to_file(sql: String, params: ArrayBufferString, keyTypes: ArrayBuffer[Int], ds: DataSource, saveToFile: String, splitter: String): DbResults = {

        var conn: java.sql.Connection = null
        var ps: PreparedStatement = null
        var rs: ResultSet = null

        val ts1 = System.currentTimeMillis

        if (log.isDebugEnabled()) {
            var paramsstr = ""
            if (params != null) paramsstr = params.mkString(",")
            log.debug("db query, sql=%s,value=%s".format(sql, paramsstr))
        }

        val localFile = new LocalFile(saveToFile)
        var totalRowCount = 0

        var hasException = false
        try {

            if (mode == DbLike.MODE_SYNC) {
                conn = tl.get()
            } else {
                conn = ds.getConnection()
            }

            ps = conn.prepareStatement(sql)

            prepare(ps, params, keyTypes)

            rs = ps.executeQuery()

            val metaData = rs.getMetaData()
            val columnCount = metaData.getColumnCount()

            var fdt: java.text.SimpleDateFormat = null

            while (rs.next()) {
                val row = new ArrayBufferAny()
                var i = 1
                while (i <= columnCount) {
                    val colType = metaData.getColumnType(i)

                    var value: Any = null
                    var tmpValue: Any = null

                    colType match {
                        case Types.DATE =>
                            if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            tmpValue = rs.getTimestamp(i)
                            if (tmpValue != null) {
                                value = fdt.format(tmpValue.asInstanceOf[java.util.Date])
                            }
                        case Types.TIMESTAMP =>
                            if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            tmpValue = rs.getTimestamp(i)
                            if (tmpValue != null)
                                value = fdt.format(tmpValue.asInstanceOf[java.util.Date])
                        case _ =>
                            value = rs.getString(i)
                    }

                    row += value
                    i += 1
                }

                totalRowCount += 1
                val line = row.mkString(splitter)
                localFile.writeLine(line)
            }

            val results = new ArrayBuffer[ArrayBufferAny]()
            val ret = new DbResults(totalRowCount, results)
            ret

        } catch {
            case e: Throwable =>
                hasException = true
                var paramsstr = ""
                if (params != null) paramsstr = params.mkString(",")
                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db query exception, e=%s,sql=%s,value=%s,sqlCode=%d".format(e.getMessage, sql, paramsstr, sqlCode))
                new DbResults(-1, sqlCode)
        } finally {

            if (localFile != null) localFile.close()

            closeResultSet(rs)
            rs = null
            closeStatement(ps)
            ps = null

            val ts = System.currentTimeMillis - ts1
            if (ts >= longTimeSql) {
                log.warn("long time sql, sql=" + sql + ", ts=[" + ts + "]ms")
            }

            if (hasException) {

                if (mode == DbLike.MODE_SYNC) {
                    mustRollback()
                }

            }
            if (mode != DbLike.MODE_SYNC) {
                closeConnection(conn)
                conn = null
            }
        }

    }

    def query_db(sql: String, ds: DataSource, callback: QueryCallback): Int = {

        var conn: java.sql.Connection = null
        var ps: Statement = null
        var rs: ResultSet = null

        if (log.isDebugEnabled()) {
            log.debug("db query, sql=%s".format(sql))
        }

        val ts1 = System.currentTimeMillis

        var hasException = false
        try {

            conn = ds.getConnection()

            ps = conn.createStatement()

            rs = ps.executeQuery(sql)

            val metaData = rs.getMetaData()
            val columnCount = metaData.getColumnCount()

            var totalRowCount = 0

            var fdt: java.text.SimpleDateFormat = null
            while (rs.next()) {
                val row = new ArrayBufferString()
                var i = 1
                while (i <= columnCount) {
                    val colType = metaData.getColumnType(i)

                    var value: String = null
                    var tmpValue: Any = null

                    colType match {
                        case Types.DATE =>
                            if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            tmpValue = rs.getTimestamp(i)
                            if (tmpValue != null) {
                                value = fdt.format(tmpValue.asInstanceOf[java.util.Date])
                            }
                        case Types.TIMESTAMP =>
                            if (fdt == null) fdt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            tmpValue = rs.getTimestamp(i)
                            if (tmpValue != null)
                                value = fdt.format(tmpValue.asInstanceOf[java.util.Date])
                        case _ =>
                            value = rs.getString(i)
                    }

                    row += value
                    i += 1
                }
                totalRowCount += 1

                val continue = callback.process(totalRowCount, row)
                if (!continue) {
                    return totalRowCount
                }
            }

            totalRowCount

        } catch {
            case e: Throwable =>
                hasException = true
                log.error("db query exception, e=%s,sql=%s".format(e.getMessage, sql))
                -1
        } finally {
            closeResultSet(rs)
            rs = null
            closeStatement(ps)
            ps = null
            closeConnection(conn)
            conn = null

            val ts = System.currentTimeMillis - ts1
            if (ts >= longTimeSql) {
                log.warn("long time sql, sql=" + sql + ", ts=[" + ts + "]ms")
            }
        }

    }

    def update_db(sql: String, params: ArrayBufferString, keyTypes: ArrayBuffer[Int], dslist: ArrayBuffer[DataSource], dbIdx: Int, insert_id: Boolean = false): DbResults = {

        var conn: java.sql.Connection = null
        var ps: PreparedStatement = null

        if (log.isDebugEnabled()) {
            var paramsstr = ""
            if (params != null) paramsstr = params.mkString(",")

            log.debug("db update, sql=%s,value=%s".format(sql, paramsstr))
        }

        val ts1 = System.currentTimeMillis

        var ds = dslist(dbIdx)

        var hasException = false

        try {

            if (mode == DbLike.MODE_SYNC) {
                conn = tl.get()
            } else {
                conn = ds.getConnection()
                conn.setAutoCommit(true)
            }

            if (insert_id)
                ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
            else
                ps = conn.prepareStatement(sql)

            prepare(ps, params, keyTypes)

            val rows = ps.executeUpdate()

            val ret = new DbResults(rows)
            if (insert_id) {
                ret.insert_ids = ArrayBufferString()
                val rs = ps.getGeneratedKeys()
                if (rs.next()) {
                    ret.insert_ids += String.valueOf(rs.getLong(1))
                }
            }
            ret

        } catch {
            case e: Throwable =>
                hasException = true
                var paramsstr = ""
                if (params != null) paramsstr = params.mkString(",")

                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db update exception, e=%s,sql=%s,value=%s,sqlCode=%d".format(e.getMessage, sql, paramsstr, sqlCode))
                new DbResults(-1, sqlCode)
        } finally {
            closeStatement(ps)
            ps = null

            val ts = System.currentTimeMillis - ts1
            if (ts >= longTimeSql) {
                log.warn("long time sql, sql=" + sql + ", ts=[" + ts + "]ms")
            }

            if (hasException) {

                if (mode == DbLike.MODE_SYNC) {
                    mustRollback()
                }

            }
            if (mode != DbLike.MODE_SYNC) {
                closeConnection(conn)
                conn = null
            }
        }
    }

    def update_db_multi(sqls_buff: ArrayBuffer[String], params_buff: ArrayBuffer[ArrayBufferString],
                        keytypes_buff: ArrayBuffer[ArrayBuffer[Int]],
                        dslist: ArrayBuffer[DataSource], dbIdx: Int, insert_id: Boolean = false): DbResults = {

        var conn: java.sql.Connection = null
        var ps_buff = new ArrayBuffer[PreparedStatement]()
        var ds = dslist(dbIdx)

        if (log.isDebugEnabled()) {

            for (i <- 0 until sqls_buff.size) {
                val sql = sqls_buff(i)
                val params = params_buff(i)
                var paramsstr = ""
                if (params != null) paramsstr = params.mkString(",")

                log.debug("db update multi #%d, sql=%s,value=%s".format(i + 1, sql, paramsstr))
            }

        }
        val ts1 = System.currentTimeMillis

        var hasException = false

        var totalrows = 0

        try {

            if (mode == DbLike.MODE_SYNC) {
                conn = tl.get()
            } else {
                conn = ds.getConnection()
                conn.setAutoCommit(false)
            }

            var sqlii = 0

            val insert_ids = ArrayBufferString()
            while (sqlii < sqls_buff.size) {

                var ps: PreparedStatement = null
                if (insert_id)
                    ps = conn.prepareStatement(sqls_buff(sqlii), Statement.RETURN_GENERATED_KEYS)
                else
                    ps = conn.prepareStatement(sqls_buff(sqlii))

                val params = params_buff(sqlii)
                val keytypes = keytypes_buff(sqlii)
                prepare(ps, params, keytypes)

                ps_buff += ps
                val rows = ps.executeUpdate()

                if (insert_id) {
                    val rs = ps.getGeneratedKeys()
                    while (rs.next()) {
                        insert_ids += String.valueOf(rs.getLong(1))
                    }
                }

                totalrows += rows
                sqlii += 1
            }

            val results = new DbResults(totalrows)
            results.insert_ids = insert_ids

            if (mode != DbLike.MODE_SYNC) {
                conn.commit()
                conn.setAutoCommit(true)
            }

            results

        } catch {
            case e: Throwable => {
                hasException = true

                var paramsstr = ""
                if (params_buff != null) paramsstr = params_buff.mkString("^_^")

                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db update multi exception, e=%s,sqls=%s,values=%s,sqlcode=%d".format(e.getMessage, sqls_buff.mkString("^_^"), paramsstr, sqlCode))
                val results = new DbResults(-1, sqlCode)
                results
            }
        } finally {
            closeStatements(ps_buff)
            ps_buff.clear()

            val ts = System.currentTimeMillis - ts1
            if (ts >= longTimeSql) {
                log.warn("long time sql, sqls=" + sqls_buff.mkString("^_^") + ", ts=[" + ts + "]ms")
            }

            if (hasException) {

                if (mode == DbLike.MODE_SYNC) {
                    mustRollback()
                } else {
                    try { conn.rollback() } catch { case e: Throwable => log.error("rollback error") };
                    try { conn.setAutoCommit(true) } catch { case e: Throwable => log.error("set autocommit error") };
                }

            }

            if (mode != DbLike.MODE_SYNC) {
                closeConnection(conn)
                conn = null
            }
        }

    }

    def update_db_batch(sql: String, params_buff: ArrayBuffer[ArrayBufferString],
                        keytypes: ArrayBuffer[Int],
                        dslist: ArrayBuffer[DataSource], dbIdx: Int, insert_id: Boolean = false): DbResults = {

        var conn: java.sql.Connection = null
        var ps: PreparedStatement = null

        var ds = dslist(dbIdx)

        if (log.isDebugEnabled()) {
            log.debug("db update batch, sql=%s".format(sql))
        }
        val ts1 = System.currentTimeMillis

        var hasException = false

        try {

            if (mode == DbLike.MODE_SYNC) {
                conn = tl.get()
            } else {
                conn = ds.getConnection()
                conn.setAutoCommit(false)
            }

            if (insert_id)
                ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
            else
                ps = conn.prepareStatement(sql)

            var i = 0
            while (i < params_buff.size) {
                val params = params_buff(i)
                prepare(ps, params, keytypes)
                ps.addBatch()
                i += 1
            }

            // SUCCESS_NO_INFO = -2, EXECUTE_FAILED = -3

            val rows = ps.executeBatch()
            var totalrows = 0
            i = 0
            while (i < rows.size) {
                if (rows(i) >= 0) {
                    totalrows += rows(i)
                }
                i += 1
            }

            val results = new DbResults(totalrows)
            if (insert_id) {
                results.insert_ids = ArrayBufferString()
                val rs = ps.getGeneratedKeys()
                while (rs.next()) {
                    results.insert_ids += String.valueOf(rs.getLong(1))
                }
            }

            if (mode != DbLike.MODE_SYNC) {
                conn.commit()
                conn.setAutoCommit(true)
            }

            results

        } catch {
            case e: Throwable => {
                hasException = true
                val sqlCode = ErrorCodeUtils.parseSqlCode(e)
                log.error("db update batch exception, e=%s,sql=%s,sqlCode=%d".format(e.getMessage, sql, sqlCode))
                val results = new DbResults(-1, sqlCode)
                results
            }
        } finally {
            closeStatement(ps)
            ps = null

            val ts = System.currentTimeMillis - ts1
            if (ts >= longTimeSql) {
                log.warn("long time sql, sql=" + sql + ", ts=[" + ts + "]ms")
            }

            if (hasException) {

                if (mode == DbLike.MODE_SYNC) {
                    mustRollback()
                } else {
                    try { conn.rollback() } catch { case e: Throwable => log.error("rollback error") };
                    try { conn.setAutoCommit(true) } catch { case e: Throwable => log.error("set autocommit error") };
                }

            }

            if (mode != DbLike.MODE_SYNC) {
                closeConnection(conn)
                conn = null
            }
        }

    }

    def checkBindConnection() {
        val conn = tl.get()
        if (conn == null)
            throw new RuntimeException("beginTransaction not called")
    }

    def beginTransaction(ds: DataSource) {

        var conn = tl.get()
        if (conn != null)
            throw new RuntimeException("beginTransaction already called")

        conn = ds.getConnection()
        conn.setAutoCommit(false)
        tl.set(conn)
    }

    def commit() {

        var conn = tl.get()
        if (conn == null)
            throw new RuntimeException("beginTransaction not called")

        try {
            tl.remove()
            conn.commit()
            conn.setAutoCommit(true)
        } catch {
            case e: Throwable =>
                mustRollback()
                throw e
        } finally {
            closeConnection(conn)
            conn = null
        }

    }

    def rollback() {

        var conn = tl.get()
        if (conn == null) return // allow rollback to be called more than one time

        try {
            tl.remove()
            conn.rollback()
            conn.setAutoCommit(true)
        } finally {
            closeConnection(conn)
            conn = null
        }

    }

    def mustRollback() {
        try {
            rollback()
        } catch {
            case e: Throwable =>
                log.error("rollback error")
        }
    }

    def closeResultSet(rs: java.sql.ResultSet) {
        if (rs != null) {
            try {
                rs.close();
            } catch {
                case e: Throwable =>
                    log.error("close result error")
            };
        }
    }

    def closeStatement(ps: java.sql.Statement) {
        if (ps != null) {
            try {
                ps.close();
            } catch {
                case e: Throwable =>
                    log.error("close statement error")
            };
        }
    }

    def closeStatement(ps: java.sql.PreparedStatement) {
        if (ps != null) {
            try {
                ps.close();
            } catch {
                case e: Throwable =>
                    log.error("close statement error")
            };
        }
    }

    def closeStatements(ps_buff: ArrayBuffer[PreparedStatement]) {
        var i = 0
        while (i < ps_buff.size) {
            val ps = ps_buff(i)
            closeStatement(ps)
            i += 1
        }
    }

    def closeConnection(conn: java.sql.Connection) {
        if (conn != null) {
            try {
                conn.close();
            } catch {
                case e: Throwable =>
                    log.error("close connection error")
            };
        }
    }

}

object ErrorCodeUtils extends Logging {

    /*

    ORA-01012 not logged on
    ORA-00028 your session has been killed
    ORA-01092 ORACLE instance terminated. Disconnection forced.
    ORA-01093 ALTER DATABASE CLOSE only permitted with no sessions connected
    ORA-01094 ALTER DATABASE CLOSE in progress. Connections not permitted
    ORA-02134 Invalid runtime context.

    ora-03113 end-of-file on communication channel
    ORA-03114 not connected to ORACLE
    ORA-01090 shutdown   in   progress   -   connection   is   not   permitted
    ORA-01034 ORACLE not available
    ora-12560 tns protocol adapter error

    17003   ORA-17003 invalid column index

    ora-00001 unique constraint
    ora-01400 cannot insert null into
    ORA-01722 invalid number
    ora-02291 integrity constraint
    ora-02292 integrity constraint violated child record found

    17002   ORA-17002 Connection   reset
    17447   ORA-17447 OALL8 is in an inconsistent state.

    54      ora-00054 resource busy and acquire with nowait specified
    8177    ora-08177 can't serialize access for this transaction
    60      ora-00060 deadlock detected

    ORA-01683: unable to extend index BOSS.IDX_TBPAYLOGHIS_MOVETIME partition PAYLOG1 by 1024 in tablespace INDEX01
    ORA-01653

    spring dataAccessResourceFailureCodes

    <bean id="MS-SQL" class="org.springframework.jdbc.support.SQLErrorCodes">
        <property name="databaseProductName">
            <value>Microsoft SQL Server</value>
        </property>
        <property name="badSqlGrammarCodes">
            <value>156,170,207,208</value>
        </property>
        <property name="permissionDeniedCodes">
            <value>229</value>
        </property>
        <property name="duplicateKeyCodes">
            <value>2601,2627</value>
        </property>
        <property name="dataIntegrityViolationCodes">
            <value>544,8114,8115</value>
        </property>
        <property name="dataAccessResourceFailureCodes">
            <value>4060</value>
        </property>
        <property name="cannotAcquireLockCodes">
            <value>1222</value>
        </property>
        <property name="deadlockLoserCodes">
            <value>1205</value>
        </property>
    </bean>

    <bean id="MySQL" class="org.springframework.jdbc.support.SQLErrorCodes">
        <property name="badSqlGrammarCodes">
            <value>1054,1064,1146</value>
        </property>
        <property name="dataIntegrityViolationCodes">
            <value>630,839,840,893,1062,1169,1215,1216,1217,1451,1452,1557</value>
        </property>
        <property name="dataAccessResourceFailureCodes">
            <value>1</value>
        </property>
        <property name="cannotAcquireLockCodes">
            <value>1205</value>
        </property>
        <property name="deadlockLoserCodes">
            <value>1213</value>
        </property>
    </bean>

    <bean id="Oracle" class="org.springframework.jdbc.support.SQLErrorCodes">
        <property name="badSqlGrammarCodes">
            <value>900,903,904,917,936,942,17006</value>
        </property>
        <property name="invalidResultSetAccessCodes">
            <value>17003</value>
        </property>
        <property name="dataIntegrityViolationCodes">
            <value>1,1400,1722,2291,2292</value>
        </property>
        <property name="dataAccessResourceFailureCodes">
            <value>17002,17447</value>
        </property>
        <property name="cannotAcquireLockCodes">
            <value>54</value>
        </property>
        <property name="cannotSerializeTransactionCodes">
            <value>8177</value>
        </property>
        <property name="deadlockLoserCodes">
            <value>60</value>
        </property>
    </bean>

    12505 ORA-12505, TNS:listener does not currently know of SID given in connect descriptor
    ORA-01017: invalid username/password; logon denied
    */

    val oracleErrorCodeReg = """^.*ORA\-([0-9]+),.*$""".r
    val mysqlErrorCodeReg = """^.*ERROR ([0-9]+):.*$""".r
    val sqlserverErrorCodeReg1 = """^.*错误([0-9]+):.*$""".r
    val sqlserverErrorCodeReg2 = """^.*error([0-9]+):.*$""".r

    val timeoutReg1 = """^.*Timeout waiting for idle object.*$""".r
    val connReg1 = """^.*Cannot create PoolableConnectionFactory.*$""".r
    val connReg2 = """^.*Connection refused.*$""".r
    val connReg3 = """^.*No suitable driver.*$""".r

    val unknown_sql_errorcode = 99999997
    val timeout_errorcode = 99999998
    val connect_errorcode = 99999999

    val oracleConnectionErrorCodes = scala.Array(1012, 1017, 28, 1092, 1093, 1094, 2134, 3113, 3114, 1090, 1034, 12505, 12560, 17002, 17447, 1683, 1653)
    val mysqlConnectionErrorCodes = scala.Array(1045, 1)
    val sqlserverConnectionErrorCodes = scala.Array(4060)

    def mergeLines(ss: String): String = {
        var sss = ss.replace("\n\r", " ").replace("\r", " ").replace("\n", " ")
        sss
    }

    def parseErrorCode(ds: DataSource, e: Throwable): Int = {
        val sqlCode = parseSqlCode(e)
        sqlCodeToErrorCode(ds, sqlCode)
    }

    def parseSqlCode(e: Throwable): Int = {

        val sqle = fetchSQLException(e)
        if (sqle == null) return unknown_sql_errorcode

        var code = sqle.getErrorCode // errorCode may be not correct
        if (code != 0) return code
        if (sqle.getMessage == null) return unknown_sql_errorcode

        val ss = mergeLines(sqle.getMessage)
        if (log.isDebugEnabled) {
            log.debug("sql e:" + ss)
        }

        ss match {
            case connReg1() =>
                return connect_errorcode
            case connReg2() =>
                return connect_errorcode
            case connReg3() =>
                return connect_errorcode
            case timeoutReg1() =>
                return timeout_errorcode
            case oracleErrorCodeReg(errorCode) =>
                return errorCode.toInt
            case mysqlErrorCodeReg(errorCode) =>
                return errorCode.toInt
            case sqlserverErrorCodeReg1(errorCode) =>
                return errorCode.toInt
            case sqlserverErrorCodeReg2(errorCode) =>
                return errorCode.toInt
            case _ =>
                return unknown_sql_errorcode
        }
    }

    def sqlCodeToErrorCode(ds: DataSource, sqlCode: Int): Int = {

        val dbType = DbLike.getDbType(ds)
        val codes = if (dbType == "oracle") oracleConnectionErrorCodes else if (dbType == "sqlserver") sqlserverConnectionErrorCodes else mysqlConnectionErrorCodes

        if (sqlCode == timeout_errorcode) return ResultCodes.DB_TIMEOUT
        if (sqlCode == connect_errorcode) return ResultCodes.DB_CONN_FAILED
        if (sqlCode == unknown_sql_errorcode) return ResultCodes.DB_ERROR

        for (code <- codes) {
            if (code == sqlCode) {
                return ResultCodes.DB_CONN_FAILED
            }
        }

        return ResultCodes.DB_ERROR;

    }

    def fetchSQLException(se: Throwable): SQLException = {

        var e = se
        var t: Throwable = null
        var needBreak = false

        while (!needBreak) {
            t = e.getCause()
            if (t == null) needBreak = true
            else if (t == e) needBreak = true
            else e = t
        }

        if (e.isInstanceOf[SQLException]) {
            val sqle = e.asInstanceOf[SQLException]
            return sqle
        }

        if (e.getMessage() != null) {
            return new SQLException(e.getMessage(), null, 0)
        }

        return null
    }

}

