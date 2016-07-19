package jvmdbbroker.core

import java.util.concurrent.ConcurrentHashMap
import java.nio.ByteBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ArrayBuffer
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
class ArrayBufferString extends ArrayBuffer[String]
class ArrayBufferInt extends ArrayBuffer[Int]
class ArrayBufferAny extends ArrayBuffer[Any]
class HashMapStringString extends HashMap[String,String]

class HashMapStringAny extends HashMap[String,Any] {

  def s(name:String) : String = TypeSafe.s(name,this)
  def s(name:String,defaultValue:String) : String = TypeSafe.s(name,this,defaultValue)
  def i(name:String) : Int = TypeSafe.i(name,this)
  def i(name:String,defaultValue:Int) : Int = TypeSafe.i(name,this,defaultValue)
  def m(name:String) : HashMapStringAny = TypeSafe.m(name,this)
  def ls(name:String) : ArrayBufferString = TypeSafe.ls(name,this)
  def li(name:String) : ArrayBufferInt = TypeSafe.li(name,this)
  def lm(name:String) : ArrayBufferMap  = TypeSafe.lm(name,this)

  def exists(names:String) : Boolean = TypeSafe.exists(names,this)
}
class ArrayBufferMap extends ArrayBuffer[HashMapStringAny]

class LinkedHashMapStringAny extends LinkedHashMap[String,Any] {}

object HashMapStringString {
  def apply(elems: Tuple2[String, String]*): HashMapStringString = {
    val m = new HashMapStringString()
    for( t <- elems ) m += t
    m
  }
}

object HashMapStringAny {
  def apply(elems: Tuple2[String, Any]*): HashMapStringAny = {
    val m = new HashMapStringAny()
    for( t <- elems ) m += t
    m
  }
}

object ArrayBufferString {
  def apply(elems: String*): ArrayBufferString = {
    val buff = new ArrayBufferString()
    for( t <- elems ) buff += t
    buff
  }
}
object ArrayBufferInt {
  def apply(elems: Int*): ArrayBufferInt = {
    val buff = new ArrayBufferInt()
    for( t <- elems ) buff += t
    buff
  }
}
object ArrayBufferAny {
  def apply(elems: Any*): ArrayBufferAny = {
    val buff = new ArrayBufferAny()
    for( t <- elems ) buff += t
    buff
  }
}
object ArrayBufferMap {
  def apply(elems: HashMapStringAny*): ArrayBufferMap = {
    val buff = new ArrayBufferMap()
    for( t <- elems ) buff += t
    buff
  }
}

trait Actor {
  def receive(v:Any): Unit;
}

trait RawRequestActor {}

// use the caller's thread to process request
trait SyncedActor {
  def get(requestId:String): Response;
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
  def loadParameter(pmap:HashMapStringString): Unit;
}

// doesnot receive request, but can generate new request
trait Bean {}

trait InitBean {
  def isInited(): Boolean;
}

class DummyActor extends Actor {
  def receive(v:Any) = {}
}

class SelfCheckResult(val name:String, val errorId:Long, val hasError:Boolean = false, val msg:String = "OK") {
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

  val SERVICE_TIMEOUT = -10242504  // msg invoke timeout
  val SERVICE_FULL = -10242488 // queue is full
  val SERVICE_BUSY = -10242488 // service is busy, req timeout
  val SERVICE_INTERNALERROR = -10242500

  val SOC_TIMEOUT = -10242504  // timeout
  val SOC_NETWORKERROR = -10242404 // network error
  val SOC_NOCONNECTION = -10242404 // network error
  val HTTP_RESPONSE_FORMAT_ERROR = -10242500
  val HTTP_RESULTCODE_NOT_FOUND = -10242500

  val DB_ERROR = -10242500
  val DB_TIMEOUT = -10242504
  val DB_CONN_FAILED = -10242404

  val CACHE_NOT_FOUND = -10245404 // key not found
  val CACHE_TIMEOUT = -10242504 // timeout
  val CACHE_FAILED = -10242404 // network error
  val CACHE_UPDATEFAILED = -10242500  // not network error, used in set, delete
  val CACHE_KEY_EMPTY = -10242400
  val CACHE_VALUE_EMPTY = -10242400

  val MQ_IO_ERROR = -10242500
  val QUEUENAME_NOT_FOUND = -10242500

}

class AvenueData (
    val flag: Int,
    val serviceId : Int,
    val msgId : Int,
    val sequence : Int,
    val mustReach : Int,
    val encoding : Int,
    val code : Int,
    var xhead : ByteBuffer, // may be changed by sos
    val body : ByteBuffer
) {
  override def toString() = "sequence=%d,serviceId=%d,msgId=%d,code=%d,xhead.length=%d,body.length=%d".format(
      sequence,serviceId,msgId,code,xhead.limit,body.limit)

}

class RawRequest( val requestId:String, val data:AvenueData,val connId:String, val sender: Actor ) {
  val receivedTime = System.currentTimeMillis
  var persistId = 0L // for "must reach" message

  def remoteIp() : String = {
      if( connId == null || connId == "" ) return null
      val p = connId.indexOf(":")
      if( p == -1 ) return null
      connId.substring(0,p)
  }

}

class RawResponse(val data:AvenueData,val connId:String ) {

  val receivedTime = System.currentTimeMillis
  var remoteAddr = ""

  def this( data:AvenueData, rawReq : RawRequest ) {
    this(data,rawReq.connId)
  }

}

class Request (

    val requestId : String,
    val connId : String,
    val sequence : Int,
    val encoding : Int,

    val serviceId : Int,
    val msgId : Int,

    val xhead : HashMapStringAny,
    val body : HashMapStringAny,
    val sender: Actor
) {

  var receivedTime = System.currentTimeMillis
  var parentServiceId = 0
  var parentMsgId = 0
  var expireTimeout = 600000 // 10 minutes, for receiver to check if expired, if expired drop it immediately
  var persistId = 0L // for "must reach" message
  var toAddr : String = null  // used to select a special soc connection

  def s(name:String) : String = body.s(name)
  def s(name:String,defaultValue:String) : String = body.s(name,defaultValue)

  def i(name:String) : Int = body.i(name)
  def i(name:String,defaultValue:Int) : Int = body.i(name,defaultValue)
  def m(name:String) : HashMapStringAny = body.m(name)
  def ls(name:String) : ArrayBufferString = body.ls(name)
  def li(name:String) : ArrayBufferInt = body.li(name)
  def lm(name:String) : ArrayBufferMap  = body.lm(name)

  def exists(names:String) : Boolean = TypeSafe.exists(names,body)

  def xs(name:String) : String = xhead.s(name)
  def xs(name:String,defaultValue:String) : String = xhead.s(name,defaultValue)
  def xi(name:String) : Int = xhead.i(name)
  def xls(name:String) : ArrayBufferString = xhead.ls(name)

  def remoteIp() : String = {
      if( connId == null || connId == "" ) return null
      val p = connId.indexOf(":")
      if( p == -1 ) return null
      connId.substring(0,p)
  }
  def remoteAddr() : String = {
      if( connId == null || connId == "" ) return null
      val p = connId.lastIndexOf(":")
      if( p == -1 ) return null
      connId.substring(0,p)
  }
  def clientIp() : String = {
      val gsInfos = xls("gsInfos")
      if( gsInfos == null || gsInfos.size == 0 ) return null
      val gsInfo = gsInfos(0)  
      val p = gsInfo.lastIndexOf(":")
      if( p == -1 ) return null
      gsInfo.substring(0,p)
  }

  override def toString() = "requestId=%s,connId=%s,sequence=%d,serviceId=%d,msgId=%d,xhead=%s,body=%s,receivedTime=%d".format(
      requestId,connId,sequence,serviceId,msgId,xhead.mkString(";"),body.mkString(";"),receivedTime)
}

class Response (

    val requestId : String,
    val connId : String,
    val sequence : Int,
    val encoding : Int,

    val serviceId : Int,
    val msgId : Int,

    var code : Int,
    var body : HashMapStringAny,

    val sender : Actor
) {

  val receivedTime = System.currentTimeMillis
  var remoteAddr = ""

  def this( code : Int, body : HashMapStringAny, req : Request ) {
    this(req.requestId,req.connId,req.sequence,req.encoding,req.serviceId,req.msgId,
      code, body,req.sender)
  }

  override def toString() = "requestId=%s,connId=%s,sequence=%d,serviceId=%d,msgId=%d,code=%d,body=%s".format(
      requestId,connId,sequence,serviceId,msgId,code,body.mkString(";"))

}

trait RequestFilter {
  def filter(req: Request): Unit;
}

trait ResponseFilter {
  def filter(res: Response, req: Request): Unit;
}

object InvokeResult {
  def success(requestId:String) = new InvokeResult(requestId,0,new HashMapStringAny())
  def timeout(requestId:String) = new InvokeResult(requestId,ResultCodes.SERVICE_TIMEOUT,new HashMapStringAny())
  def failed(requestId:String) = new InvokeResult(requestId,ResultCodes.SERVICE_NOT_FOUND,new HashMapStringAny())
}

class InvokeResult(val requestId:String, val code:Int, val res:HashMapStringAny) {

  def s(name:String) : String = res.s(name)
  def s(name:String,defaultValue:String) : String = res.s(name,defaultValue)

  def i(name:String) : Int = res.i(name)
  def m(name:String) : HashMapStringAny = res.m(name)
  def ls(name:String) : ArrayBufferString = res.ls(name)
  def li(name:String) : ArrayBufferInt = res.li(name)
  def lm(name:String) : ArrayBufferMap  = res.lm(name)

  def exists(names:String) : Boolean = TypeSafe.exists(names,res)

  override def toString() = "requestId=%s,code=%d,body=%s".format(requestId,code,res.mkString(";"))
}

class SocRequest (
    val requestId : String,
    val serviceId : Int,
    val msgId : Int,
    val body : HashMapStringAny,
    val encoding : Int = AvenueCodec.ENCODING_UTF8,
    val xhead : HashMapStringAny = new HashMapStringAny()
) {

  var connId : String = null
  val receivedTime = System.currentTimeMillis

  override def toString() = "requestId=%s,serviceId=%d,msgId=%d,body=%s,encoding=%d,xhead=%s,connId=%s".format(
      requestId,serviceId,msgId,body.mkString(";"),encoding,xhead.mkString(";"),connId)
}

class SocResponse (
    val requestId : String,
    val code : Int,
    val body : HashMapStringAny
) {

  var connId : String = null
  val receivedTime = System.currentTimeMillis
  var remoteAddr = ""

  override def toString() = "requestId=%s,code=%d,body=%s,connId=%s".format(
      requestId,code,body.mkString(";"),connId)

}

class RawRequestResponseInfo(val rawReq: RawRequest, val rawRes: RawResponse)
//class RawRequestErrorResponse(val rawReq:RawRequest,val code:Int)
class RequestResponseInfo(val req: Request, val res: Response, val logVars: HashMapStringAny = null )
class SocRequestResponseInfo(val req: SocRequest, val res: SocResponse)

class RawRequestAckInfo(val rawReq: RawRequest)
class RequestAckInfo(val req: Request)
class SocRequestAckInfo(val req: SocRequest)

class HttpSosRequest (
    val requestId : String,
    val connId : String,
    val serviceId : Int,
    val msgId : Int,
    val xhead : HashMapStringAny,
    var params: String
) {
  var receivedTime = System.currentTimeMillis

  override def toString() = "requestId=%s,connId=%s,serviceId=%d,msgId=%d,params=%s,receivedTime=%d".format(
      requestId,connId,serviceId,msgId,params,receivedTime)
}

class HttpSosResponse (
    val requestId : String,
    val code : Int,
    var content : String
) {
  var receivedTime = System.currentTimeMillis

  override def toString() = "requestId=%s,code=%d,content=%s".format(requestId,code,content)
}

class HttpSosRequestResponseInfo(val req: HttpSosRequest, val res: HttpSosResponse)

class CachedSplitter(val splitter:String) {

  private val strToArrayMap = new ConcurrentHashMap[String,Array[String]]()

  def strToArray(s:String):Array[String] = {
    val ss = strToArrayMap.get(s)
    if( ss == null ) {
      val nss = s.split(splitter)
      var i = 0
      while( i < nss.size) {
        nss(i) = nss(i).trim
        i+=1
      }
      strToArrayMap.put(s,nss)
      nss
    } else {
      ss
    }
  }

}

object CachedSplitter{
  val commaSplitter = new CachedSplitter(",")
  val tabSplitter = new CachedSplitter("\t")
  val colonSplitter = new CachedSplitter(":")
  val blankSplitter = new CachedSplitter(" ")
  val dotSplitter = new CachedSplitter("\\.")
}

object TypeSafe {

  def exists(names:String,body:HashMapStringAny) : Boolean = {
    val ss = CachedSplitter.commaSplitter.strToArray(names)
    for(s <- ss ) {
      if( !body.contains(s) ) return false
    }
    return true
  }

  def s(name:String,body:HashMapStringAny) : String = {
    val value = body.getOrElse(name,null)
    anyToString(value)
  }

  def s(name:String,body:HashMapStringAny,defaultValue:String) : String = {
    val value = s(name,body)
    if( value == null ) return defaultValue
    else return value
  }

  def i(name:String,body:HashMapStringAny) : Int = {
    val value = body.getOrElse(name,null)
    anyToInt(value)
  }

  def i(name:String,body:HashMapStringAny,defaultValue:Int) : Int = {
    val value = body.getOrElse(name,null)
    if( value == null ) return defaultValue
    anyToInt(value)
  }

  def m(name:String,body:HashMapStringAny) : HashMapStringAny = {
    val value = body.getOrElse(name,null)

    if( value == null ) return null
    if( value.isInstanceOf[ HashMapStringAny ] ) return value.asInstanceOf[ HashMapStringAny  ]
    throw new RuntimeException("wrong data type, name="+name)
  }
  def ls(name:String,body:HashMapStringAny) : ArrayBufferString = {
    val value = body.getOrElse(name,null)

    if( value == null ) return null
    if( value.isInstanceOf[ ArrayBufferString ] ) return value.asInstanceOf[ ArrayBufferString  ]
    if( value.isInstanceOf[ ArrayBufferAny ] ) {
      val aa = value.asInstanceOf[ ArrayBufferAny ]
      val as = new ArrayBufferString()
      for( a <- aa if a.isInstanceOf[String]  ) as +=a.asInstanceOf[String]
      return as
    }    
    throw new RuntimeException("wrong data type, name="+name)
  }
  def li(name:String,body:HashMapStringAny) : ArrayBufferInt = {
    val value = body.getOrElse(name,null)

    if( value == null ) return null
    if( value.isInstanceOf[ ArrayBufferInt ] ) return value.asInstanceOf[ ArrayBufferInt ]
    if( value.isInstanceOf[ ArrayBufferAny ] ) {
      val aa = value.asInstanceOf[ ArrayBufferAny ]
      val ai = new ArrayBufferInt()
      for( a <- aa if a.isInstanceOf[Int]  ) ai +=a.asInstanceOf[Int]
      return ai
    }    
    throw new RuntimeException("wrong data type, name="+name)
  }
  def lm(name:String,body:HashMapStringAny) : ArrayBufferMap = {
    val value = body.getOrElse(name,null)

    if( value == null ) return null
    if( value.isInstanceOf[ ArrayBufferMap ] ) return value.asInstanceOf[ ArrayBufferMap ]
    if( value.isInstanceOf[ ArrayBufferAny ] ) {
      val aa = value.asInstanceOf[ ArrayBufferAny ]
      val am = new ArrayBufferMap()
      for( a <- aa if a.isInstanceOf[HashMapStringAny] ) am += a.asInstanceOf[HashMapStringAny]
      return am
    }
    throw new RuntimeException("wrong data type, name="+name)
  }

  def anyToInt(value:Any):Int = {

      if( value == null ) return 0

      value match {
        case i: Int => i
        case l: Long => l.toInt
        case s: Short => s.toInt
        case b: Byte => b.toInt
        case bl: Boolean => if( bl ) 1 else 0
        case str: String =>

          if( str == "" ) {
            0
          } else {
            try {
              str.toInt
            } catch {
              case _ : Throwable => 0
            }
          }
        case _ =>
          try {
            value.toString.toInt
          } catch {
            case _ : Throwable => 0
          }
      }
  }

  def anyToString(value:Any):String ={

      if( value == null ) return null

      value match {
        case str: String => str
        case _ => value.toString
      }
  }

  def anyToString(value:Any,defaultValue:String):String ={
      val v = anyToString(value)
      if( v == null ) return defaultValue
      v
  }
}

