package scalabpe.core

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import scala.math.BigDecimal

import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.LoggerFactory;

class HashMapStringString extends HashMap[String, String]

class HashMapStringAny extends HashMap[String, Any] {

    def v(name: String): Any = getOrElse(name, null)
    def s(name: String): String = TypeSafe.s(name, this)
    def s(name: String, defaultValue: String): String = TypeSafe.s(name, this, defaultValue)
    def ns(name: String): String = TypeSafe.ns(name, this)
    def ns(name: String, defaultValue: String): String = TypeSafe.ns(name, this, defaultValue)
    def i(name: String): Int = TypeSafe.i(name, this)
    def i(name: String, defaultValue: Int): Int = TypeSafe.i(name, this, defaultValue)
    def l(name: String): Long = TypeSafe.l(name, this)
    def d(name: String): Double = TypeSafe.d(name, this)
    def bd(name: String): BigDecimal = TypeSafe.bd(name, this)
    def m(name: String): HashMapStringAny = TypeSafe.m(name, this)
    def nm(name: String): HashMapStringAny = TypeSafe.nm(name, this)
    def ls(name: String): ArrayBufferString = TypeSafe.ls(name, this)
    def nls(name: String): ArrayBufferString = TypeSafe.nls(name, this)
    def li(name: String): ArrayBufferInt = TypeSafe.li(name, this)
    def nli(name: String): ArrayBufferInt = TypeSafe.nli(name, this)
    def ll(name: String): ArrayBufferLong = TypeSafe.ll(name, this)
    def nll(name: String): ArrayBufferLong = TypeSafe.nll(name, this)
    def ld(name: String): ArrayBufferDouble = TypeSafe.ld(name, this)
    def nld(name: String): ArrayBufferDouble = TypeSafe.nld(name, this)
    def lm(name: String): ArrayBufferMap = TypeSafe.lm(name, this)
    def nlm(name: String): ArrayBufferMap = TypeSafe.nlm(name, this)

    def exists(names: String): Boolean = TypeSafe.exists(names, this)
}

class ArrayBufferString extends ArrayBuffer[String] {
    override def filter(f: (String) => Boolean): ArrayBufferString = {
        ArrayBufferString(super.filter(f))
    }
    override def filterNot(f: (String) => Boolean): ArrayBufferString = {
        ArrayBufferString(super.filterNot(f))
    }

    override def sortWith(f: (String, String) => Boolean): ArrayBufferString = {
        ArrayBufferString(super.sortWith(f))
    }
}
class ArrayBufferInt extends ArrayBuffer[Int] {
    override def filter(f: (Int) => Boolean): ArrayBufferInt = {
        ArrayBufferInt(super.filter(f))
    }
    override def filterNot(f: (Int) => Boolean): ArrayBufferInt = {
        ArrayBufferInt(super.filterNot(f))
    }

    override def sortWith(f: (Int, Int) => Boolean): ArrayBufferInt = {
        ArrayBufferInt(super.sortWith(f))
    }
}
class ArrayBufferLong extends ArrayBuffer[Long] {
    override def filter(f: (Long) => Boolean): ArrayBufferLong = {
        ArrayBufferLong(super.filter(f))
    }
    override def filterNot(f: (Long) => Boolean): ArrayBufferLong = {
        ArrayBufferLong(super.filterNot(f))
    }

    override def sortWith(f: (Long, Long) => Boolean): ArrayBufferLong = {
        ArrayBufferLong(super.sortWith(f))
    }
}
class ArrayBufferDouble extends ArrayBuffer[Double] {
    override def filter(f: (Double) => Boolean): ArrayBufferDouble = {
        ArrayBufferDouble(super.filter(f))
    }
    override def filterNot(f: (Double) => Boolean): ArrayBufferDouble = {
        ArrayBufferDouble(super.filterNot(f))
    }

    override def sortWith(f: (Double, Double) => Boolean): ArrayBufferDouble = {
        ArrayBufferDouble(super.sortWith(f))
    }
}
class ArrayBufferMap extends ArrayBuffer[HashMapStringAny] {
    override def filter(f: (HashMapStringAny) => Boolean): ArrayBufferMap = {
        ArrayBufferMap(super.filter(f))
    }
    override def filterNot(f: (HashMapStringAny) => Boolean): ArrayBufferMap = {
        ArrayBufferMap(super.filterNot(f))
    }

    override def sortWith(f: (HashMapStringAny, HashMapStringAny) => Boolean): ArrayBufferMap = {
        ArrayBufferMap(super.sortWith(f))
    }
}

class ArrayBufferAny extends ArrayBuffer[Any]

object HashMapStringString {
    def apply(elems: Tuple2[String, String]*): HashMapStringString = {
        val m = new HashMapStringString()
        for (t <- elems) m += t
        m
    }
    def apply(l: HashMap[String, String]): HashMapStringString = {
        val map = new HashMapStringString()
        map ++= l
        map
    }
}

object HashMapStringAny {
    def apply(elems: Tuple2[String, Any]*): HashMapStringAny = {
        val m = new HashMapStringAny()
        for (t <- elems) m += t
        m
    }
    def apply(l: HashMap[String, Any]): HashMapStringAny = {
        val map = new HashMapStringAny()
        map ++= l
        map
    }
}

object ArrayBufferString {
    def apply(elems: String*): ArrayBufferString = {
        val buff = new ArrayBufferString()
        for (t <- elems) buff += t
        buff
    }
    def apply(l: ArrayBuffer[String]): ArrayBufferString = {
        val buff = new ArrayBufferString()
        buff ++= l
        buff
    }
}
object ArrayBufferInt {
    def apply(elems: Int*): ArrayBufferInt = {
        val buff = new ArrayBufferInt()
        for (t <- elems) buff += t
        buff
    }
    def apply(l: ArrayBuffer[Int]): ArrayBufferInt = {
        val buff = new ArrayBufferInt()
        buff ++= l
        buff
    }
}

object ArrayBufferLong {
    def apply(elems: Long*): ArrayBufferLong = {
        val buff = new ArrayBufferLong()
        for (t <- elems) buff += t
        buff
    }
    def apply(l: ArrayBuffer[Long]): ArrayBufferLong = {
        val buff = new ArrayBufferLong()
        buff ++= l
        buff
    }
}

object ArrayBufferDouble {
    def apply(elems: Double*): ArrayBufferDouble = {
        val buff = new ArrayBufferDouble()
        for (t <- elems) buff += t
        buff
    }
    def apply(l: ArrayBuffer[Double]): ArrayBufferDouble = {
        val buff = new ArrayBufferDouble()
        buff ++= l
        buff
    }
}
object ArrayBufferMap {
    def apply(elems: HashMapStringAny*): ArrayBufferMap = {
        val buff = new ArrayBufferMap()
        for (t <- elems) buff += t
        buff
    }
    def apply(l: ArrayBuffer[HashMapStringAny]): ArrayBufferMap = {
        val buff = new ArrayBufferMap()
        buff ++= l
        buff
    }
}
object ArrayBufferAny {
    def apply(elems: Any*): ArrayBufferAny = {
        val buff = new ArrayBufferAny()
        for (t <- elems) buff += t
        buff
    }
    def apply(l: ArrayBuffer[Any]): ArrayBufferAny = {
        val buff = new ArrayBufferAny()
        buff ++= l
        buff
    }
}

class LinkedHashMapStringAny extends LinkedHashMap[String, Any] {

    def v(name: String): Any = getOrElse(name, null)
    def s(name: String): String = TypeSafe.s(name, this)
    def s(name: String, defaultValue: String): String = TypeSafe.s(name, this, defaultValue)
    def ns(name: String): String = TypeSafe.ns(name, this)
    def ns(name: String, defaultValue: String): String = TypeSafe.ns(name, this, defaultValue)

}

trait Actor {
    def receive(v: Any): Unit;
}

trait RawRequestActor {}

// use the caller's thread to process request
trait SyncedActor {
    def get(requestId: String): Response;
}

trait Refreshable {
    def refresh(): Unit;
}

trait Logging {
    val log = LoggerFactory.getLogger(this.getClass.getName);
}

trait AfterInit {
    def afterInit(): Unit;
}

trait Closable {
    def close(): Unit;
}

trait BeforeClose {
    def beforeClose(): Unit;
}

trait InitHook {
    def loadParameter(pmap: HashMapStringString): Unit;
}
trait RegDisHook {
    def updateXml(xml: String): String;
}

// doesnot receive request, but can generate new request
trait Bean {}

trait InitBean {
    def isInited(): Boolean;
}

class DummyActor extends Actor {
    def receive(v: Any) = {}
}

class SelfCheckResult(val name: String, val errorId: Long, val hasError: Boolean = false, val msg: String = "OK") {
    val timestamp = System.currentTimeMillis
}

trait SelfCheckLike {
    def selfcheck(): ArrayBuffer[SelfCheckResult];
}

trait Dumpable {
    def dump(): Unit;
}

object ResultCodes {

    val SERVICE_NOT_FOUND = -10242405
    val MSG_NOT_FOUND = -10242405

    val TLV_DECODE_ERROR = -10242400
    val TLV_ENCODE_ERROR = -10242400

    val SERVICE_TIMEOUT = -10242504 // msg invoke timeout
    val SERVICE_FULL = -10242488 // queue is full
    val SERVICE_BUSY = -10242488 // service is busy, req timeout
    val SERVICE_INTERNALERROR = -10242500

    val SOC_TIMEOUT = -10242504 // timeout
    val SOC_NETWORKERROR = -10242404 // network error
    val SOC_NOCONNECTION = -10242404 // network error

    val DB_ERROR = -10242500
    val DB_TIMEOUT = -10242504
    val DB_CONN_FAILED = -10242404

    val CACHE_NOT_FOUND = -10245404 // key not found
    val CACHE_TIMEOUT = -10242504 // timeout
    val CACHE_FAILED = -10242404 // network error
    val CACHE_UPDATEFAILED = -10242500 // not network error, used in set, delete
    val CACHE_KEY_EMPTY = -10242400
    val CACHE_VALUE_EMPTY = -10242400
}

class AvenueData(
        val flag: Int,
        val version: Int,
        val serviceId: Int,
        val msgId: Int,
        val sequence: Int,
        val mustReach: Int,
        val encoding: Int,
        val code: Int,
        val xhead: ChannelBuffer,
        val body: ChannelBuffer) {
    override def toString() = "sequence=%d,serviceId=%d,msgId=%d,code=%d,xhead.length=%d,body.length=%d".format(
        sequence, serviceId, msgId, code, xhead.writerIndex, body.writerIndex)

}

class RawRequest(val requestId: String, val data: AvenueData, val connId: String, val sender: Actor) {
    val receivedTime = System.currentTimeMillis
    var persistId = 0L // for "must reach" message

    def remoteIp(): String = {
        if (connId == null || connId == "") return null
        val p = connId.indexOf(":")
        if (p == -1) return null
        connId.substring(0, p)
    }

}

class RawResponse(val data: AvenueData, val connId: String) {
    val receivedTime = System.currentTimeMillis
    var remoteAddr = ""

    def this(data: AvenueData, rawReq: RawRequest) {
        this(data, rawReq.connId)
    }

}

class Request(
        val requestId: String,
        val connId: String,
        val sequence: Int,
        val encoding: Int,

        val serviceId: Int,
        val msgId: Int,

        val xhead: HashMapStringAny,
        val body: HashMapStringAny,
        val sender: Actor) {

    var receivedTime = System.currentTimeMillis
    var parentServiceId = 0
    var parentMsgId = 0
    var expireTimeout = 600000 // 10 minutes, for receiver to check if expired, if expired drop it immediately
    var persistId = 0L // for "must reach" message
    var toAddr: String = null // used to select a special soc connection
    var version = 0 // not specified

    def v(name: String): Any = body.getOrElse(name, null)
    def s(name: String): String = body.s(name)
    def s(name: String, defaultValue: String): String = body.s(name, defaultValue)
    def ns(name: String): String = body.ns(name)
    def ns(name: String, defaultValue: String): String = body.ns(name, defaultValue)
    def i(name: String): Int = body.i(name)
    def i(name: String, defaultValue: Int): Int = body.i(name, defaultValue)
    def l(name: String): Long = body.l(name)
    def d(name: String): Double = body.d(name)
    def bd(name: String): BigDecimal = body.bd(name)

    def m(name: String): HashMapStringAny = body.m(name)
    def nm(name: String): HashMapStringAny = body.nm(name)
    def ls(name: String): ArrayBufferString = body.ls(name)
    def nls(name: String): ArrayBufferString = body.nls(name)
    def li(name: String): ArrayBufferInt = body.li(name)
    def nli(name: String): ArrayBufferInt = body.nli(name)
    def ll(name: String): ArrayBufferLong = body.ll(name)
    def nll(name: String): ArrayBufferLong = body.nll(name)
    def ld(name: String): ArrayBufferDouble = body.ld(name)
    def nld(name: String): ArrayBufferDouble = body.nld(name)
    def lm(name: String): ArrayBufferMap = body.lm(name)
    def nlm(name: String): ArrayBufferMap = body.nlm(name)

    def exists(names: String): Boolean = TypeSafe.exists(names, body)

    def xs(name: String): String = xhead.s(name)
    def xs(name: String, defaultValue: String): String = xhead.s(name, defaultValue)
    def xi(name: String): Int = xhead.i(name)
    def xls(name: String): ArrayBufferString = xhead.ls(name)

    def remoteIp(): String = {
        if (connId == null || connId == "") return null
        val p = connId.indexOf(":")
        if (p == -1) return null
        connId.substring(0, p)
    }
    def remoteAddr(): String = {
        if (connId == null || connId == "") return null
        val p = connId.lastIndexOf(":")
        if (p == -1) return null
        connId.substring(0, p)
    }
    def clientIp(): String = {
        val firstAddr = xs(Xhead.KEY_FIRST_ADDR)
        if (firstAddr == null || firstAddr == "") return null
        val p = firstAddr.lastIndexOf(":")
        if (p == -1) return null
        firstAddr.substring(0, p)
    }

    override def toString() = "requestId=%s,connId=%s,sequence=%d,serviceId=%d,msgId=%d,xhead=%s,body=%s,receivedTime=%d".format(
        requestId, connId, sequence, serviceId, msgId, xhead.mkString(";"), body.mkString(";"), receivedTime)
}

class Response(

        val requestId: String,
        val connId: String,
        val sequence: Int,
        val encoding: Int,

        val serviceId: Int,
        val msgId: Int,

        var code: Int,
        var body: HashMapStringAny,

        val sender: Actor) {

    val receivedTime = System.currentTimeMillis
    var remoteAddr = ""

    def this(code: Int, body: HashMapStringAny, req: Request) {
        this(req.requestId, req.connId, req.sequence, req.encoding, req.serviceId, req.msgId,
            code, body, req.sender)
    }

    override def toString() = "requestId=%s,connId=%s,sequence=%d,serviceId=%d,msgId=%d,code=%d,body=%s".format(
        requestId, connId, sequence, serviceId, msgId, code, body.mkString(";"))

}

trait RequestFilter {
    def filter(req: Request): Unit;
}

trait ResponseFilter {
    def filter(res: Response, req: Request): Unit;
}

object InvokeResult {
    def success(requestId: String) = new InvokeResult(requestId, 0, new HashMapStringAny())
    def timeout(requestId: String) = new InvokeResult(requestId, ResultCodes.SERVICE_TIMEOUT, new HashMapStringAny())
    def failed(requestId: String) = new InvokeResult(requestId, ResultCodes.SERVICE_NOT_FOUND, new HashMapStringAny())
}

class InvokeResult(val requestId: String, val code: Int, val res: HashMapStringAny) {

    def v(name: String): Any = res.getOrElse(name, null)
    def s(name: String): String = res.s(name)
    def s(name: String, defaultValue: String): String = res.s(name, defaultValue)
    def ns(name: String): String = res.ns(name)
    def ns(name: String, defaultValue: String): String = res.ns(name, defaultValue)
    def i(name: String): Int = res.i(name)
    def i(name: String, defaultValue: Int): Int = res.i(name, defaultValue)
    def l(name: String): Long = res.l(name)
    def d(name: String): Double = res.d(name)
    def bd(name: String): BigDecimal = res.bd(name)

    def m(name: String): HashMapStringAny = res.m(name)
    def nm(name: String): HashMapStringAny = res.nm(name)
    def ls(name: String): ArrayBufferString = res.ls(name)
    def nls(name: String): ArrayBufferString = res.nls(name)
    def li(name: String): ArrayBufferInt = res.li(name)
    def nli(name: String): ArrayBufferInt = res.nli(name)
    def ll(name: String): ArrayBufferLong = res.ll(name)
    def nll(name: String): ArrayBufferLong = res.nll(name)
    def ld(name: String): ArrayBufferDouble = res.ld(name)
    def nld(name: String): ArrayBufferDouble = res.nld(name)
    def lm(name: String): ArrayBufferMap = res.lm(name)
    def nlm(name: String): ArrayBufferMap = res.nlm(name)

    def exists(names: String): Boolean = TypeSafe.exists(names, res)

    override def toString() = "requestId=%s,code=%d,body=%s".format(requestId, code, res.mkString(";"))
}

class SocRequest(
        val requestId: String,
        val serviceId: Int,
        val msgId: Int,
        val body: HashMapStringAny,
        val encoding: Int = AvenueCodec.ENCODING_UTF8,
        val xhead: HashMapStringAny = new HashMapStringAny()) {

    var connId: String = null
    val receivedTime = System.currentTimeMillis

    override def toString() = "requestId=%s,serviceId=%d,msgId=%d,body=%s,encoding=%d,xhead=%s,connId=%s".format(
        requestId, serviceId, msgId, body.mkString(";"), encoding, xhead.mkString(";"), connId)
}

class SocResponse(
        val requestId: String,
        val code: Int,
        val body: HashMapStringAny) {

    var connId: String = null
    val receivedTime = System.currentTimeMillis
    var remoteAddr = ""

    override def toString() = "requestId=%s,code=%d,body=%s,connId=%s".format(
        requestId, code, body.mkString(";"), connId)

}

class RawRequestResponseInfo(val rawReq: RawRequest, val rawRes: RawResponse)
class RequestResponseInfo(val req: Request, val res: Response, val logVars: HashMapStringAny = null)
class SocRequestResponseInfo(val req: SocRequest, val res: SocResponse)

class RawRequestAckInfo(val rawReq: RawRequest)
class RequestAckInfo(val req: Request)
class SocRequestAckInfo(val req: SocRequest)

class HttpSosRequest(
        val requestId: String,
        val connId: String,
        val serviceId: Int,
        val msgId: Int,
        val xhead: HashMapStringAny,
        var params: String) {
    var receivedTime = System.currentTimeMillis

    override def toString() = "requestId=%s,connId=%s,serviceId=%d,msgId=%d,params=%s,receivedTime=%d".format(
        requestId, connId, serviceId, msgId, params, receivedTime)
}

class HttpSosResponse(
        val requestId: String,
        val code: Int,
        var content: String) {
    var receivedTime = System.currentTimeMillis

    override def toString() = "requestId=%s,code=%d,content=%s".format(requestId, code, content)
}

class HttpSosRequestResponseInfo(val req: HttpSosRequest, val res: HttpSosResponse)

class CachedSplitter(val splitter: String) {

    private val strToArrayMap = new ConcurrentHashMap[String, Array[String]]()

    def strToArray(s: String): Array[String] = {
        val ss = strToArrayMap.get(s)
        if (ss == null) {
            val nss = s.split(splitter)
            var i = 0
            while (i < nss.size) {
                nss(i) = nss(i).trim
                i += 1
            }
            strToArrayMap.put(s, nss)
            nss
        } else {
            ss
        }
    }

}

object CachedSplitter {
    val commaSplitter = new CachedSplitter(",")
    val tabSplitter = new CachedSplitter("\t")
    val colonSplitter = new CachedSplitter(":")
    val blankSplitter = new CachedSplitter(" ")
    val dotSplitter = new CachedSplitter("\\.")
}

object TypeSafe {

    def exists(names: String, body: HashMapStringAny): Boolean = {
        val ss = CachedSplitter.commaSplitter.strToArray(names)
        for (s <- ss) {
            if (!body.contains(s)) return false
        }
        return true
    }

    def isTrue(s: String): Boolean = {
        s.toLowerCase match {
            case "1" | "y" | "t" => true
            case "yes" | "true"  => true
            case _               => false
        }
    }

    def s(name: String, body: LinkedHashMapStringAny): String = {
        val value = body.getOrElse(name, null)
        anyToString(value)
    }

    def s(name: String, body: LinkedHashMapStringAny, defaultValue: String): String = {
        val value = s(name, body)
        if (value == null) defaultValue
        else value
    }

    def ns(name: String, body: LinkedHashMapStringAny): String = {
        val value = body.getOrElse(name, null)
        val v = anyToString(value)
        if (v == null) ""
        else v
    }

    def ns(name: String, body: LinkedHashMapStringAny, defaultValue: String): String = {
        val value = ns(name, body)
        if (value == "") defaultValue
        else value
    }    
    
    def s(name: String, body: HashMapStringAny): String = {
        val value = body.getOrElse(name, null)
        anyToString(value)
    }

    def s(name: String, body: HashMapStringAny, defaultValue: String): String = {
        val value = s(name, body)
        if (value == null) defaultValue
        else value
    }

    def ns(name: String, body: HashMapStringAny): String = {
        val value = body.getOrElse(name, null)
        val v = anyToString(value)
        if (v == null) ""
        else v
    }

    def ns(name: String, body: HashMapStringAny, defaultValue: String): String = {
        val value = ns(name, body)
        if (value == "") defaultValue
        else value
    }

    def i(name: String, body: HashMapStringAny): Int = {
        val value = body.getOrElse(name, null)
        anyToInt(value)
    }

    def i(name: String, body: HashMapStringAny, defaultValue: Int): Int = {
        val value = body.getOrElse(name, null)
        if (value == null) return defaultValue
        anyToInt(value)
    }

    def l(name: String, body: HashMapStringAny): Long = {
        val value = body.getOrElse(name, null)
        anyToLong(value)
    }

    def d(name: String, body: HashMapStringAny): Double = {
        val value = body.getOrElse(name, null)
        anyToDouble(value)
    }

    def bd(name: String, body: HashMapStringAny): BigDecimal = {
        val value = body.getOrElse(name, null)
        anyToBigDecimal(value)
    }

    def m(name: String, body: HashMapStringAny): HashMapStringAny = {
        val value = body.getOrElse(name, null)

        if (value == null) return null

        value match {
            case m: HashMapStringAny =>
                return m
            case _ =>
                throw new RuntimeException("wrong data type, name=" + name)
        }
    }

    def nm(name: String, body: HashMapStringAny): HashMapStringAny = {
        val r = m(name, body)
        if (r == null) return HashMapStringAny()
        r
    }
    def ls(name: String, body: HashMapStringAny): ArrayBufferString = {
        val value = body.getOrElse(name, null)

        if (value == null) return null

        value match {
            case as: ArrayBufferString =>
                return as
            case aa: ArrayBuffer[_] =>
                val as = new ArrayBufferString()
                for (a <- aa) as += anyToString(a)
                return as
            case _ =>
                throw new RuntimeException("wrong data type, name=" + name)
        }
    }
    def nls(name: String, body: HashMapStringAny): ArrayBufferString = {
        val l = ls(name, body)
        if (l == null) return ArrayBufferString()
        l
    }
    def li(name: String, body: HashMapStringAny): ArrayBufferInt = {
        val value = body.getOrElse(name, null)

        if (value == null) return null

        value match {
            case ai: ArrayBufferInt =>
                return ai
            case aa: ArrayBuffer[_] =>
                val ai = new ArrayBufferInt()
                for (a <- aa) ai += anyToInt(a)
                return ai
            case _ =>
                throw new RuntimeException("wrong data type, name=" + name)
        }
    }
    def nli(name: String, body: HashMapStringAny): ArrayBufferInt = {
        val l = li(name, body)
        if (l == null) return ArrayBufferInt()
        l
    }
    def ll(name: String, body: HashMapStringAny): ArrayBufferLong = {
        val value = body.getOrElse(name, null)

        if (value == null) return null

        value match {
            case al: ArrayBufferLong =>
                return al
            case aa: ArrayBuffer[_] =>
                val al = new ArrayBufferLong()
                for (a <- aa) al += anyToLong(a)
                return al
            case _ =>
                throw new RuntimeException("wrong data type, name=" + name)
        }
    }
    def nll(name: String, body: HashMapStringAny): ArrayBufferLong = {
        val l = ll(name, body)
        if (l == null) return ArrayBufferLong()
        l
    }
    def ld(name: String, body: HashMapStringAny): ArrayBufferDouble = {
        val value = body.getOrElse(name, null)

        if (value == null) return null

        value match {
            case ad: ArrayBufferDouble =>
                return ad
            case aa: ArrayBuffer[_] =>
                val ad = new ArrayBufferDouble()
                for (a <- aa) ad += anyToLong(a)
                return ad
            case _ =>
                throw new RuntimeException("wrong data type, name=" + name)
        }
    }
    def nld(name: String, body: HashMapStringAny): ArrayBufferDouble = {
        val l = ld(name, body)
        if (l == null) return ArrayBufferDouble()
        l
    }
    def lm(name: String, body: HashMapStringAny): ArrayBufferMap = {
        val value = body.getOrElse(name, null)

        if (value == null) return null

        value match {
            case am: ArrayBufferMap =>
                return am
            case aa: ArrayBuffer[_] =>
                val am = new ArrayBufferMap()
                for (a <- aa) {
                    a match {
                        case a: HashMapStringAny =>
                            am += a
                        case _ =>
                            throw new RuntimeException("wrong data type, name=" + name)
                    }
                }
                return am
            case _ =>
                throw new RuntimeException("wrong data type, name=" + name)
        }
    }
    def nlm(name: String, body: HashMapStringAny): ArrayBufferMap = {
        val l = lm(name, body)
        if (l == null) return ArrayBufferMap()
        l
    }

    def anyToInt(value: Any): Int = {

        if (value == null) return 0

        value match {
            case i: Int      => i
            case l: Long     => l.toInt
            case s: Short    => s.toInt
            case b: Byte     => b.toInt
            case f: Float    => f.toInt
            case d: Double   => d.toInt
            case bl: Boolean => if (bl) 1 else 0
            case str: String =>

                if (str == "") {
                    0
                } else {
                    try {
                        str.toInt
                    } catch {
                        case _: Throwable => 0
                    }
                }
            case _ =>
                try {
                    value.toString.toInt
                } catch {
                    case _: Throwable => 0
                }
        }
    }

    def anyToLong(value: Any): Long = {

        if (value == null) return 0

        value match {
            case l: Long     => l
            case i: Int      => i
            case s: Short    => s
            case b: Byte     => b
            case f: Float    => f.toLong
            case d: Double   => d.toLong
            case bl: Boolean => if (bl) 1 else 0
            case str: String =>

                if (str == "") {
                    0
                } else {
                    try {
                        str.toLong
                    } catch {
                        case _: Throwable => 0
                    }
                }
            case _ =>
                try {
                    value.toString.toLong
                } catch {
                    case _: Throwable => 0
                }
        }
    }

    def anyToDouble(value: Any): Double = {

        if (value == null) return 0.0

        value match {
            case d: Double   => d
            case f: Float    => f
            case i: Int      => i
            case l: Long     => l
            case s: Short    => s
            case b: Byte     => b
            case bl: Boolean => if (bl) 1 else 0
            case str: String =>

                if (str == "") {
                    0
                } else {
                    try {
                        str.toDouble
                    } catch {
                        case _: Throwable => 0
                    }
                }
            case _ =>
                try {
                    value.toString.toDouble
                } catch {
                    case _: Throwable => 0
                }
        }
    }

    def anyToBigDecimal(value: Any): BigDecimal = {

        if (value == null) return 0.0

        value match {
            case i: Int      => BigDecimal(i)
            case l: Long     => BigDecimal(l)
            case s: Short    => BigDecimal(s)
            case b: Byte     => BigDecimal(b)
            case f: Float    => BigDecimal(f.toDouble)
            case d: Double   => BigDecimal(d)
            case bl: Boolean => if (bl) BigDecimal(1) else BigDecimal(0)
            case str: String =>

                if (str == "") {
                    BigDecimal(0)
                } else {
                    try {
                        BigDecimal(str)
                    } catch {
                        case _: Throwable => BigDecimal(0)
                    }
                }
            case _ =>
                try {
                    BigDecimal(value.toString)
                } catch {
                    case _: Throwable => BigDecimal(0)
                }
        }
    }

    def anyToString(value: Any): String = {

        if (value == null) return null

        value match {
            case str: String => str
            case _           => value.toString
        }
    }

    def anyToString(value: Any, defaultValue: String): String = {
        val v = anyToString(value)
        if (v == null) return defaultValue
        v
    }
}

