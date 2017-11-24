package scalabpe.plugin.http

import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.LinkedHashMap
import scala.xml.Elem
import scala.xml.Node
import scala.xml.XML

import org.jboss.netty.buffer.DynamicChannelBuffer
import org.jboss.netty.handler.codec.http.DefaultHttpRequest
import org.jboss.netty.handler.codec.http.HttpMethod
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.handler.codec.http.HttpVersion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.CachedSplitter
import scalabpe.core.CryptHelper
import scalabpe.core.Dumpable
import scalabpe.core.HashMapStringAny
import scalabpe.core.HashMapStringString
import scalabpe.core.HttpClient4Netty
import scalabpe.core.Logging
import scalabpe.core.NettyHttpClient
import scalabpe.core.Request
import scalabpe.core.RequestResponseInfo
import scalabpe.core.Response
import scalabpe.core.ResultCodes
import scalabpe.core.SocRequest
import scalabpe.core.SocRequestResponseInfo
import scalabpe.core.SocResponse
import scalabpe.core.TlvCodec
import scalabpe.core.TlvCodecs
import scalabpe.core.TlvType

// used by user
trait HttpClient {
    def send(req: Request, timeout: Int): Unit;
    def send(socReq: SocRequest, timeout: Int): Unit;
}

case class HttpClientResponse(sequence: Int, httpRes: HttpResponse)
case class HttpClientTimeout(sequence: Int)
case class HttpClientNetworkError(sequence: Int)

class HttpClientCacheData(val data: Any, val sendTime: Long, val timeout: Int, var sendTimes: Int) {
    def this(req: Request, timeout: Int) { this(req, System.currentTimeMillis, timeout, 0) }
    def this(socReq: SocRequest, timeout: Int) { this(socReq, System.currentTimeMillis, timeout, 0) }
}

class HttpMsgRequest(val key: String, val defaultValue: String, val ignored: Boolean, val ns: String = "") {

    override def toString(): String = {
        "key=%s,defaultValue=%s,ignored=%s,ns=%s".format(key, defaultValue, ignored, ns)
    }
}
class HttpMsgResponse(val key: String, val tp: Int, val tlvType: TlvType, var path: String) {
    override def toString(): String = {
        "key=%s,type=%d,path=%s,tlvType=%s".format(key, tp, path, tlvType.toString)
    }
}

object HttpMsgDefine {

    val GET = 1
    val POST = 2
    val HEAD = 3
    val PUT = 4
    val DELETE = 5

    val MIMETYPE_FORM = "application/x-www-form-urlencoded"
    val MIMETYPE_JSON = "application/json"
    val MIMETYPE_JSON2 = "application/vnd.api+json"
    val MIMETYPE_XML = "text/xml"

    val MIMETYPE_XML2 = "application/xml"
    val MIMETYPE_HTML = "text/html"
    val MIMETYPE_PLAIN = "text/plain"

    def parseMethod(s: String): Int = {
        s match {
            case "GET" | "get"       => GET
            case "POST" | "post"     => POST
            case "HEAD" | "head"     => HEAD
            case "PUT" | "put"       => PUT
            case "DELETE" | "delete" => DELETE
            case _                   => POST
        }
    }
}

class HttpAhtCfg(
        val serviceId: Int,
        val msgId: Int,
        val serverUrl: String,
        val serverUrlSuffix: String,
        val needSignature: String,
        val signatureKey: String,
        val method: String,
        val requestContentType: String,
        val charSet: String,
        val parseContentOnError: String,
        val pluginObj: HttpPlugin,
        val wsReqSuffix: String = "",
        val wsResSuffix: String = "",
        val wsReqWrap: String = "",
        val wsResWrap: String = "",
        val wsWrapNs: String = "",
        val wsSOAPAction: String = "",
        val wsNs: String = "") {

    override def toString(): String = {
        "serviceId=%d,msgId=%d,serverUrl=%s,serverUrlSuffix=%s,needSignature=%s,signatureKey=%s,method=%s,requestContentType=%s,charSet=%s,pluginObj=%s".
            format(serviceId, msgId, serverUrl, serverUrlSuffix, needSignature, signatureKey, method, requestContentType, charSet, pluginObj)
    }
}

class HttpMsgDefine(

        val serviceId: Int,
        val msgId: Int,
        val name: String,

        val reqFields: ArrayBuffer[HttpMsgRequest],
        val resFields: ArrayBuffer[HttpMsgResponse],

        val ssl: Boolean,
        val host: String,
        val path: String,

        val needSignature: Boolean,
        val signatureKey: String,
        val method: Int,
        val requestContentType: String,
        val charSet: String,
        val parseContentOnError:Boolean,
        val pluginObj: HttpPlugin,

        val signatureKeyField: String,
        val customUrlField: String,

        val resultCodeField: String,

        val wsReqSuffix: String = "",
        val wsResSuffix: String = "",
        val wsReqWrap: String = "",
        val wsResWrap: String = "",
        val wsWrapNs: String = "",
        val wsSOAPAction: String = "",
        val wsNs: String = "") {

    override def toString(): String = {
        "serviceId=%d,msgId=%d,host=%s,path=%s,needSignature=%s,signatureKey=%s,method=%d,requestContentType=%s,charSet=%s,pluginObj=%s,signatureKeyField=%s,customUrlField=%s,resultCodeField=%s,reqFields=%s,resFields=%s".
            format(serviceId, msgId, host, path, needSignature, signatureKey, method, requestContentType, charSet, pluginObj, signatureKeyField, customUrlField, resultCodeField, reqFields.mkString("#"), resFields.mkString("#"))
    }
}

class HttpClientImpl(
        val cfgNode: Node,
        val codecs: TlvCodecs,
        val receiver_f: (Any) => Unit,
        val connectTimeout: Int = 15000,
        val timerInterval: Int = 100,
        val retryTimes: Int = 1,
        val maxContentLength: Int = 1048576 ) extends HttpClient with HttpClient4Netty with Logging with Dumpable {

    var nettyHttpClient: NettyHttpClient = _
    val generator = new AtomicInteger(1)

    val msgs = new HashMap[String, HttpMsgDefine]()
    val dataMap = new ConcurrentHashMap[Int, HttpClientCacheData]()

    val mapper = new ObjectMapper()

    val r1 = """<[a-zA-Z0-9]+:""".r
    val r2 = """</[a-zA-Z0-9]+:""".r
    val r3 = """ [a-zA-Z0-9]+:""".r
    val rempty = "[ \t\n]+".r

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("dataMap.size=").append(dataMap.size).append(",")

        log.info(buff.toString)

        nettyHttpClient.dump
    }

    def init() {

        initMsgs()

        nettyHttpClient = new NettyHttpClient(this,
            connectTimeout,
            timerInterval,
            maxContentLength)

        log.info("httpclient started")
    }

    def initMsgs() {

        val ahtCfgs = new HashMap[String, HttpAhtCfg]()

        val types = cfgNode \ "Service" \ "Item"

        val ahtServiceIds = new HashSet[Int]()

        for (t <- types) {

            val serviceId = (t \ "ServiceId").text.toInt
            val msgId = (t \ "MsgId").text.toInt

            val serverUrl = (t \ "ServerUrl").text
            val serverUrlSuffix = (t \ "ServerUrlSuffix").text
            var needSignature = (t \ "NeedSignature").text.toLowerCase
            var signatureKey = (t \ "Signature").text
            if (signatureKey == "") signatureKey = (t \ "SignatureKey").text
            val method = (t \ "Method").text.toUpperCase
            val requestContentType = (t \ "RequestContentType").text
            val charSet = (t \ "CharSet").text
            val parseContentOnError = (t \ "ParseContentOnError").text

            val wsReqSuffix = (t \ "WSReqSuffix").text
            val wsResSuffix = (t \ "WSResSuffix").text
            val wsReqWrap = (t \ "WSReqWrap").text
            val wsResWrap = (t \ "WSResWrap").text
            val wsWrapNs = (t \ "WSWrapNs").text
            val wsSOAPAction = (t \ "WSSOAPAction").text
            val wsNs = rempty.replaceAllIn((t \ "WSNs").text, " ")

            val plugin = (t \ "Plugin").text
            var pluginObj: HttpPlugin = null

            if (plugin != "") {

                try {

                    val obj = Class.forName(plugin).getConstructors()(0).newInstance()
                    if (!obj.isInstanceOf[HttpPlugin]) {
                        throw new RuntimeException("plugin %s is not HttpPlugin".format(plugin))
                    }
                    pluginObj = obj.asInstanceOf[HttpPlugin]

                } catch {
                    case e: Exception =>
                        log.error("plugin {} cannot be loaded", plugin)
                        throw e
                }

            }

            val cfg = new HttpAhtCfg(
                serviceId, msgId,
                serverUrl, serverUrlSuffix,
                needSignature, signatureKey, method, requestContentType, charSet, parseContentOnError, pluginObj,
                wsReqSuffix, wsResSuffix, wsReqWrap, wsResWrap, wsWrapNs, wsSOAPAction, wsNs)

            ahtServiceIds.add(serviceId)
            ahtCfgs.put(serviceId + ":" + msgId, cfg)

        }

        var s = ""

        for (serviceId <- ahtServiceIds) {

            val codec = codecs.findTlvCodec(serviceId)
            if (codec == null) {
                throw new RuntimeException("service not found in aht cfg serviceId=%d".format(serviceId))
            }

            val msgIds = codec.msgKeyToTypeMapForReq.keys

            for (msgId <- msgIds) {

                val name = codec.msgIdToOrigNameMap.getOrElse(msgId, "")
                val ahtCfg = ahtCfgs.getOrElse(serviceId + ":" + msgId, null)
                val defaultAhtCfg = ahtCfgs.getOrElse(serviceId + ":-1", null)

                if (ahtCfg == null && defaultAhtCfg == null) {
                    throw new RuntimeException("msg not defined in aht cfg serviceId=%d,msgId=%d".format(serviceId, msgId))
                }

                val msgAttributes = codec.msgAttributes.getOrElse(msgId, null)
                val reqNameMap = codec.msgKeysForReq.getOrElse(msgId, null)
                val resNameMap = codec.msgKeysForRes.getOrElse(msgId, null)
                val keyToTypeMapReq = codec.msgKeyToTypeMapForReq.getOrElse(msgId, null)
                val keyToTypeMapRes = codec.msgKeyToTypeMapForRes.getOrElse(msgId, null)

                var signatureKeyField = ""
                var customUrlField = ""
                var resultCodeField = ""

                val reqs = new ArrayBuffer[HttpMsgRequest]()
                val ress = new ArrayBuffer[HttpMsgResponse]()

                for (key <- reqNameMap) {
                    val defaultValue = msgAttributes.getOrElse("req-" + key + "-default", null)
                    val typeKey = keyToTypeMapReq.getOrElse(key, null)
                    val tlvType = codec.typeNameToCodeMap.getOrElse(typeKey, null)
                    val isSignature = tlvType.code == 10000
                    if (isSignature) signatureKeyField = key
                    val isNotifyUrl = tlvType.code == 10001
                    if (isNotifyUrl) customUrlField = key
                    var ns = msgAttributes.getOrElse("req-" + key + "-ns", "")

                    if (key.indexOf(".") >= 0 && ns == "") {
                        ns = "*." * (key.split("\\.").size)
                        ns = ns.substring(0, ns.size - 1)
                    }

                    if (key.indexOf(".") >= 0 && ns != "") {
                        val ss = key.split("\\.")
                        val nss = ns.split("\\.")
                        if (ss.length != nss.length) {
                            throw new RuntimeException("field ns defination not correct in aht cfg serviceId=%d,msgId=%d".format(serviceId, msgId))
                        }
                    }

                    val req = new HttpMsgRequest(key, defaultValue, tlvType.code >= 10000 && tlvType.code <= 10100, ns) // ignore  10000 and 10100
                    reqs += req
                }

                for (key <- resNameMap) {
                    var path = msgAttributes.getOrElse("res-" + key + "-jsonpath", "")
                    if (path == "") path = msgAttributes.getOrElse("res-" + key + "-path", "")
                    s = msgAttributes.getOrElse("res-" + key + "-isreturnfield", "").toLowerCase
                    if (s == "") s = msgAttributes.getOrElse("res-" + key + "-isResultCode", "").toLowerCase
                    if (s == "true" || s == "yes" || s == "t" || s == "y" || s == "1") resultCodeField = key
                    val typeKey = keyToTypeMapRes.getOrElse(key, null)
                    val tlvType = codec.typeNameToCodeMap.getOrElse(typeKey, null)

                    val res = new HttpMsgResponse(key, tlvType.cls, tlvType, path)
                    ress += res
                }

                var serverUrl = ""
                if (ahtCfg != null && ahtCfg.serverUrl != "") serverUrl = ahtCfg.serverUrl
                if (serverUrl == "" && defaultAhtCfg != null && defaultAhtCfg.serverUrl != "") serverUrl = defaultAhtCfg.serverUrl
                if (ahtCfg != null && ahtCfg.serverUrlSuffix != "") serverUrl = serverUrl + ahtCfg.serverUrlSuffix

                val (ssl, host, path) = parseHostPath(serverUrl)
                if ((host == "" || path == "") && customUrlField == "") {
                    throw new RuntimeException("msg url not defined in aht cfg serviceId=%d,msgId=%d".format(serviceId, msgId))
                }

                var needSignatureStr = ""
                if (ahtCfg != null && ahtCfg.needSignature != "") needSignatureStr = ahtCfg.needSignature
                if (needSignatureStr == "" && defaultAhtCfg != null && defaultAhtCfg.needSignature != "") needSignatureStr = defaultAhtCfg.needSignature
                if (needSignatureStr == "") needSignatureStr = "false"
                s = msgAttributes.getOrElse("signature", "").toLowerCase
                if (s != "") needSignatureStr = s

                val needSignature = (needSignatureStr == "true" || needSignatureStr == "yes" || needSignatureStr == "t" || needSignatureStr == "y" || needSignatureStr == "1")

                var signatureKey = ""
                if (ahtCfg != null && ahtCfg.signatureKey != "") signatureKey = ahtCfg.signatureKey
                if (signatureKey == "" && defaultAhtCfg != null && defaultAhtCfg.signatureKey != "") signatureKey = defaultAhtCfg.signatureKey

                if (needSignature && signatureKey == "" && signatureKeyField == "") {
                    throw new RuntimeException("signatureKey not defined in aht cfg serviceId=%d,msgId=%d".format(serviceId, msgId))
                }

                var methodStr = ""
                if (ahtCfg != null && ahtCfg.method != "") methodStr = ahtCfg.method
                if (methodStr == "" && defaultAhtCfg != null && defaultAhtCfg.method != "") methodStr = defaultAhtCfg.method

                val method = HttpMsgDefine.parseMethod(methodStr)

                var requestContentType = HttpMsgDefine.MIMETYPE_FORM
                if (ahtCfg != null && ahtCfg.requestContentType != "") requestContentType = ahtCfg.requestContentType
                if (requestContentType == "" && defaultAhtCfg != null && defaultAhtCfg.requestContentType != "") requestContentType = defaultAhtCfg.requestContentType

                var charSet = ""
                if (ahtCfg != null && ahtCfg.charSet != "") charSet = ahtCfg.charSet
                if (charSet == "" && defaultAhtCfg != null && defaultAhtCfg.charSet != "") charSet = defaultAhtCfg.charSet
                if (charSet == "") charSet = "UTF-8"

                var pluginObj: HttpPlugin = null
                if (ahtCfg != null && ahtCfg.pluginObj != null) pluginObj = ahtCfg.pluginObj
                if (pluginObj == null && defaultAhtCfg != null && defaultAhtCfg.pluginObj != null) pluginObj = defaultAhtCfg.pluginObj

                var parseContentOnErrorStr = ""
                if (ahtCfg != null && ahtCfg.parseContentOnError != "") parseContentOnErrorStr = ahtCfg.parseContentOnError
                if (parseContentOnErrorStr == "" && defaultAhtCfg != null && defaultAhtCfg.parseContentOnError != "") parseContentOnErrorStr = defaultAhtCfg.parseContentOnError
                if (parseContentOnErrorStr == "") parseContentOnErrorStr = "false"

                val parseContentOnError = (parseContentOnErrorStr == "true" || parseContentOnErrorStr == "yes" || parseContentOnErrorStr == "t" || parseContentOnErrorStr == "y" || parseContentOnErrorStr == "1")

                var wsReqSuffix = ""
                if (ahtCfg != null && ahtCfg.wsReqSuffix != "") wsReqSuffix = ahtCfg.wsReqSuffix
                if (wsReqSuffix == "" && defaultAhtCfg != null && defaultAhtCfg.wsReqSuffix != "") wsReqSuffix = defaultAhtCfg.wsReqSuffix

                var wsResSuffix = ""
                if (ahtCfg != null && ahtCfg.wsResSuffix != "") wsResSuffix = ahtCfg.wsResSuffix
                if (wsResSuffix == "" && defaultAhtCfg != null && defaultAhtCfg.wsResSuffix != "") wsResSuffix = defaultAhtCfg.wsResSuffix

                var wsReqWrap = ""
                if (ahtCfg != null && ahtCfg.wsReqWrap != "") wsReqWrap = ahtCfg.wsReqWrap
                if (wsReqWrap == "" && defaultAhtCfg != null && defaultAhtCfg.wsReqWrap != "") wsReqWrap = defaultAhtCfg.wsReqWrap

                var wsResWrap = ""
                if (ahtCfg != null && ahtCfg.wsResWrap != "") wsResWrap = ahtCfg.wsResWrap
                if (wsResWrap == "" && defaultAhtCfg != null && defaultAhtCfg.wsResWrap != "") wsResWrap = defaultAhtCfg.wsResWrap

                var wsWrapNs = ""
                if (ahtCfg != null && ahtCfg.wsWrapNs != "") wsWrapNs = ahtCfg.wsWrapNs
                if (wsWrapNs == "" && defaultAhtCfg != null && defaultAhtCfg.wsWrapNs != "") wsWrapNs = defaultAhtCfg.wsWrapNs
                if (wsWrapNs == "*" || wsWrapNs == "default") wsWrapNs = ""

                var wsSOAPAction = ""
                if (ahtCfg != null && ahtCfg.wsSOAPAction != "") wsSOAPAction = ahtCfg.wsSOAPAction
                if (wsSOAPAction == "" && defaultAhtCfg != null && defaultAhtCfg.wsSOAPAction != "") wsSOAPAction = defaultAhtCfg.wsSOAPAction

                var wsNs = ""
                if (ahtCfg != null && ahtCfg.wsNs != "") wsNs = ahtCfg.wsNs
                if (wsNs == "" && defaultAhtCfg != null && defaultAhtCfg.wsNs != "") wsNs = defaultAhtCfg.wsNs

                val msg = new HttpMsgDefine(
                    serviceId, msgId, name,
                    reqs, ress,
                    ssl, host, path,
                    needSignature, signatureKey, method, requestContentType, charSet, parseContentOnError, pluginObj, 
                    signatureKeyField, customUrlField, resultCodeField,
                    wsReqSuffix, wsResSuffix, wsReqWrap, wsResWrap, wsWrapNs, wsSOAPAction, wsNs)

                if (requestContentType == HttpMsgDefine.MIMETYPE_XML && (wsResSuffix != "" || wsResWrap != "" || wsReqSuffix != "" || wsReqWrap != "")) {
                    val pathPrefix = "Body." + msg.name + wsResSuffix + "." + wsResWrap + "."
                    for (resField <- ress) {
                        resField.path = pathPrefix + resField.path
                    }
                }

                msgs.put(serviceId + ":" + msgId, msg)
            }
        }

        if (log.isDebugEnabled()) {
            for ((key, msgDefine) <- msgs) {
                log.debug("key=" + key + ",msgDefine=" + msgDefine)
            }
        }

    }

    def close() {
        nettyHttpClient.close()
        log.info("httpclient stopped")
    }

    def parseHostPath(url: String): Tuple3[Boolean, String, String] = {
        var v = (false, "", "")
        val p1 = url.indexOf("//");
        if (p1 < 0) return v
        val p2 = url.indexOf("/", p1 + 2);
        if (p2 < 0) return v
        var host = url.substring(p1 + 2, p2)
        val path = url.substring(p2)

        if (url.startsWith("https://") && host.indexOf(":") < 0)
            host = host + ":443"

        val ssl = url.startsWith("https://")
        (ssl, host, path)
    }

    def findMsg(serviceId: Int, msgId: Int): HttpMsgDefine = {
        val msg = msgs.getOrElse(serviceId + ":" + msgId, null)
        msg
    }

    def receive(sequence: Int, httpRes: HttpResponse): Unit = {

        try {
            receive(HttpClientResponse(sequence, httpRes))
        } catch {
            case e: Throwable =>
                log.error("receive exception", e)
        }
    }

    def networkError(sequence: Int) {
        try {
            receive(HttpClientNetworkError(sequence))
        } catch {
            case e: Throwable =>
                log.error("networkError callback exception", e)
        }
    }

    def timeoutError(sequence: Int) {
        try {
            receive(HttpClientTimeout(sequence))
        } catch {
            case e: Throwable =>
                log.error("timeoutError callback exception", e)
        }

    }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def generateRequestBody(msg: HttpMsgDefine, reqBody: HashMapStringAny): String = {

        val outBody = new HashMapStringString()
        var i = 0
        while (i < msg.reqFields.size) {
            val field = msg.reqFields(i)
            if (!field.ignored) {
                val value = reqBody.s(field.key, field.defaultValue)
                if (value != null) {
                    outBody.put(field.key, value)
                }
            }
            i += 1
        }

        if (msg.needSignature) {

            var signatureKey = msg.signatureKey
            if (msg.signatureKeyField != "") {
                val s = reqBody.s(msg.signatureKeyField, "")
                if (s != "") signatureKey = s
            }

            if (msg.pluginObj != null && msg.pluginObj.isInstanceOf[HttpSignPlugin]) {
                val signPlugin = msg.pluginObj.asInstanceOf[HttpSignPlugin]
                signPlugin.sign(msg, signatureKey, outBody)
            } else {
                signHps(msg, signatureKey, outBody)
            }

        }

        if (msg.requestContentType == HttpMsgDefine.MIMETYPE_XML)
            return generateSoapBody(msg, outBody)

        val bodyStr = new StringBuilder()
        var first = true

        for ((key, value) <- outBody) {
            if (!first) bodyStr.append("&")
            bodyStr.append(key + "=" + URLEncoder.encode(value, msg.charSet))
            first = false
        }

        bodyStr.toString
    }

    def parseFirstNs(s: String): String = {
        if (s == "") return ""
        val ns = s.split(" ")(0).split("=")(0)
        if (ns == "default") "" else ns
    }
    def parseAllNs(s: String): String = {
        if (s == "") return ""
        val ss = s.split(" ")
        val buff = new StringBuilder()
        for (ts <- ss) {
            val tt = ts.split("=")
            val name = if (tt(0) == "default") "" else ":" + tt(0)
            val value = tt(1)
            buff.append(""" xmlns%s="%s"""".format(name, value))
        }
        buff.toString
    }

    def generateSoapBody(msg: HttpMsgDefine, body: HashMapStringString): String = {
        val env_start = """<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>"""
        val env_end = """</soap:Body></soap:Envelope>"""
        val firstNs = parseFirstNs(msg.wsNs)
        val allNsDef = parseAllNs(msg.wsNs)
        val method_start =
            if (firstNs != "")
                """<%s:%s %s>""".format(firstNs, msg.name, allNsDef)
            else
                """<%s>""".format(msg.name)
        val method_end =
            if (firstNs != "")
                """</%s:%s>""".format(firstNs, msg.name + msg.wsReqSuffix)
            else
                """</%s>""".format(msg.name + msg.wsReqSuffix)

        val reqWrap_start =
            if (msg.wsReqWrap != "") {
                if (msg.wsWrapNs != "")
                    """<%s:%s>""".format(msg.wsWrapNs, msg.wsReqWrap)
                else
                    """<%s>""".format(msg.wsReqWrap)
            } else {
                ""
            }
        val reqWrap_end =
            if (msg.wsResWrap != "") {
                if (msg.wsWrapNs != "")
                    """</%s:%s>""".format(msg.wsWrapNs, msg.wsReqWrap)
                else
                    """</%s>""".format(msg.wsReqWrap)
            } else {
                ""
            }

        val postStr = new StringBuilder()
        postStr.append(env_start).append(method_start).append(reqWrap_start)
        postStr.append(genBodyStr(msg, body))
        postStr.append(reqWrap_end).append(method_end).append(env_end)
        postStr.toString
    }

    def genBodyStr(msg: HttpMsgDefine, body: HashMapStringString): String = {

        val map = new LinkedHashMap[String, AnyRef]()

        for (i <- 0 until msg.reqFields.size) {
            val field = msg.reqFields(i)
            if (!field.ignored) {
                var value = body.getOrElse(field.key, null)
                if (value != null) {

                    value = xmlEncodeValue(value)
                    if (field.key.indexOf(".") < 0) {
                        val nskey = (if (field.ns == "default" || field.ns == "*" || field.ns == "") "" else field.ns + ":") + field.key
                        map.put(nskey, value)
                    } else {
                        val ss = field.key.split("\\.")
                        val ns = field.ns.split("\\.")
                        var lastmap = map
                        for (kk <- 0 until ss.size) {

                            val nskey = (if (ns(kk) == "default" || ns(kk) == "*" || ns(kk) == "") "" else ns(kk) + ":") + ss(kk)
                            if (kk == ss.size - 1) {
                                lastmap.put(nskey, value)
                            } else {
                                var m = lastmap.getOrElse(nskey, null)
                                if (m == null) {
                                    m = new LinkedHashMap[String, AnyRef]()
                                    lastmap.put(nskey, m)
                                }
                                lastmap = m.asInstanceOf[LinkedHashMap[String, AnyRef]]
                            }
                        }
                    }
                }

            }
        }
        mapToStr(map)
    }

    def mapToStr(map: LinkedHashMap[String, AnyRef]): String = {

        val bodyStr = new StringBuilder()

        for ((key, value) <- map) {
            value match {

                case v: String =>

                    val s = """<%s>%s</%s>""".format(key, v, key)
                    bodyStr.append(s)

                case map: LinkedHashMap[_, _] =>

                    bodyStr.append("""<%s>""".format(key))
                    bodyStr.append(mapToStr(map.asInstanceOf[LinkedHashMap[String, AnyRef]]))
                    bodyStr.append("""</%s>""".format(key))

                case _ =>
            }

        }
        bodyStr.toString
    }

    def xmlEncodeValue(s: String) = scala.xml.Utility.escape(s)

    def signHps(msg: HttpMsgDefine, signatureKey: String, body: HashMapStringString): Unit = {

        val signMap = new java.util.TreeMap[String, String]()

        for ((key, value) <- body) {
            signMap.put(key, value)
        }

        signMap.put("signature_method", "MD5")

        val timestamp = String.valueOf(System.currentTimeMillis / 1000)
        signMap.put("timestamp", timestamp)

        val signString = new StringBuilder()

        val entries = signMap.entrySet().iterator()
        while (entries.hasNext()) {
            val entry = entries.next()
            signString.append(entry.getKey).append("=").append(entry.getValue)
        }

        signString.append(signatureKey)

        val signature = CryptHelper.md5(signString.toString)

        body.put("signature_method", "MD5")
        body.put("timestamp", timestamp)
        body.put("signature", signature)
    }

    def removeStandardPort(host: String): String = {
        if (host.indexOf(":") < 0) return host
        val ss = host.split(":")
        if (ss(1) == "80" || ss(1) == "443") {
            return ss(0)
        } else {
            return host
        }

    }

    def generateHttpRequest(msg: HttpMsgDefine, a_host: String, path: String, reqBody: HashMapStringAny): HttpRequest = {

        var host = removeStandardPort(a_host)
        val headers = generateHeaders(msg, reqBody)

        var body: String = null

        if (msg.pluginObj != null && msg.pluginObj.isInstanceOf[HttpRequestPlugin]) {
            val reqPlugin = msg.pluginObj.asInstanceOf[HttpRequestPlugin]
            body = reqPlugin.generateRequestBody(msg, reqBody)
        }
        if (body == null) {
            body = generateRequestBody(msg, reqBody)
        }

        var bodyBytes: Array[Byte] = null

        if (msg.pluginObj != null && msg.pluginObj.isInstanceOf[RawHttpRequestPlugin]) {
            val reqPlugin = msg.pluginObj.asInstanceOf[RawHttpRequestPlugin]
            bodyBytes = reqPlugin.generateRequestBody(msg, reqBody)
        }

        msg.method match {

            case HttpMsgDefine.GET =>

                var getPath = path
                if (path.indexOf("?") >= 0) getPath = getPath + "&" + body
                else getPath = getPath + "?" + body

                if (getPath.endsWith("&")) getPath = getPath.substring(0, getPath.length - 1)

                if (log.isDebugEnabled()) {
                    log.debug("get path={},", getPath)
                }

                val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, getPath)
                httpReq.setHeader("Host", host) // the host include port already
                for ((key, value) <- headers) httpReq.setHeader(key, value)
                httpReq

            case HttpMsgDefine.HEAD =>

                var getPath = path
                if (path.indexOf("?") >= 0) getPath = getPath + "&" + body
                else getPath = getPath + "?" + body

                if (getPath.endsWith("&")) getPath = getPath.substring(0, getPath.length - 1)

                if (log.isDebugEnabled()) {
                    log.debug("head path={},", getPath)
                }

                val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, getPath)
                httpReq.setHeader("Host", host) // the host include port already
                for ((key, value) <- headers) httpReq.setHeader(key, value)
                httpReq

            case HttpMsgDefine.DELETE =>

                var getPath = path
                if (path.indexOf("?") >= 0) getPath = getPath + "&" + body
                else getPath = getPath + "?" + body

                if (getPath.endsWith("&")) getPath = getPath.substring(0, getPath.length - 1)

                if (log.isDebugEnabled()) {
                    log.debug("delete path={},", getPath)
                }

                val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, getPath)
                httpReq.setHeader("Host", host) // the host include port already
                for ((key, value) <- headers) httpReq.setHeader(key, value)
                httpReq

            case HttpMsgDefine.POST =>

                val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path)

                val buffer = new DynamicChannelBuffer(2048);

                if (bodyBytes != null)
                    buffer.writeBytes(bodyBytes)
                else
                    buffer.writeBytes(body.getBytes(msg.charSet))

                if (log.isDebugEnabled()) {
                    log.debug("post path={}, content={}", path, body)
                }

                httpReq.setContent(buffer);
                httpReq.setHeader("Host", host) // the host include port already
                for ((key, value) <- headers) httpReq.setHeader(key, value)
                httpReq.setHeader("Content-Type", msg.requestContentType + "; charset=" + msg.charSet)
                httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex)
                httpReq.setHeader("User-Agent", "scalabpe aht plugin")

                if (msg.requestContentType == HttpMsgDefine.MIMETYPE_XML && msg.wsSOAPAction != "") {
                    httpReq.setHeader("SOAPAction", "\"" + msg.wsSOAPAction + "\"")
                }
                httpReq

            case HttpMsgDefine.PUT =>

                val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, path)

                val buffer = new DynamicChannelBuffer(2048);

                if (bodyBytes != null)
                    buffer.writeBytes(bodyBytes)
                else
                    buffer.writeBytes(body.getBytes(msg.charSet))

                if (log.isDebugEnabled()) {
                    log.debug("put path={}, content={}", path, body)
                }

                httpReq.setContent(buffer);
                httpReq.setHeader("Host", host) // the host include port already
                for ((key, value) <- headers) httpReq.setHeader(key, value)
                httpReq.setHeader("Content-Type", msg.requestContentType + "; charset=" + msg.charSet)
                httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex)
                httpReq.setHeader("User-Agent", "scalabpe aht plugin")

                if (msg.requestContentType == HttpMsgDefine.MIMETYPE_XML && msg.wsSOAPAction != "") {
                    httpReq.setHeader("SOAPAction", "\"" + msg.wsSOAPAction + "\"")
                }
                httpReq

            case _ =>

                null
        }

    }

    def generateHeaders(msg: HttpMsgDefine, reqBody: HashMapStringAny): HashMapStringString = {

        val outBody = new HashMapStringString()
        var i = 0
        while (i < msg.reqFields.size) {
            val field = msg.reqFields(i)
            if (field.ignored && field.key.startsWith("header_")) {
                val value = reqBody.s(field.key, field.defaultValue)
                if (value != null) {
                    outBody.put(field.key.substring(7), value)
                }
            }
            i += 1
        }
        outBody
    }

    def generateHostPath(msg: HttpMsgDefine, reqBody: HashMapStringAny): Tuple3[Boolean, String, String] = {
        var ssl = msg.ssl
        var host = msg.host
        var path = msg.path

        if (msg.customUrlField != null) {
            val s = reqBody.s(msg.customUrlField, "")
            if (s != "") {
                val (customssl, customhost, custompath) = parseHostPath(s)
                ssl = customssl
                host = customhost
                path = custompath
            }
        }

        (ssl, host, path)
    }

    def send(req: Request, timeout: Int): Unit = {

        val msg = findMsg(req.serviceId, req.msgId)
        if (msg == null) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        if (msg.pluginObj != null && msg.pluginObj.isInstanceOf[HttpPreRequestPlugin]) {
            val reqPlugin = msg.pluginObj.asInstanceOf[HttpPreRequestPlugin]
            reqPlugin.adjustRequestBody(msg, req.body)
        }

        val (ssl, host, path) = generateHostPath(msg, req.body)
        if (host == "" || path == "") {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new RequestResponseInfo(req, res))
            return
        }

        var httpReq = generateHttpRequest(msg, host, path, req.body)

        val sequence = generateSequence()
        dataMap.put(sequence, new HttpClientCacheData(req, timeout))
        nettyHttpClient.send(sequence, ssl, host, httpReq, timeout)
    }

    def send(req: SocRequest, timeout: Int): Unit = {

        val msg = findMsg(req.serviceId, req.msgId)
        if (msg == null) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new SocRequestResponseInfo(req, res))
            return
        }

        if (msg.pluginObj != null && msg.pluginObj.isInstanceOf[HttpPreRequestPlugin]) {
            val reqPlugin = msg.pluginObj.asInstanceOf[HttpPreRequestPlugin]
            reqPlugin.adjustRequestBody(msg, req.body)
        }

        val (ssl, host, path) = generateHostPath(msg, req.body)
        if (host == "" || path == "") {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR, req)
            receiver_f(new SocRequestResponseInfo(req, res))
            return
        }

        var httpReq = generateHttpRequest(msg, host, path, req.body)

        val sequence = generateSequence()
        dataMap.put(sequence, new HttpClientCacheData(req, timeout))
        nettyHttpClient.send(sequence, ssl, host, httpReq, timeout)
    }

    def createErrorResponse(code: Int, req: Request): Response = {
        val res = new Response(code, new HashMapStringAny(), req)
        res
    }
    def createErrorResponse(code: Int, req: SocRequest): SocResponse = {
        val res = new SocResponse(req.requestId, code, new HashMapStringAny())
        res
    }

    def generateResponseBody(msg: HttpMsgDefine, httpRes: HttpResponse): Tuple2[Int, HashMapStringAny] = {

        /*
        if( log.isDebugEnabled() ) {
            log.debug("httpRes={},",httpRes.toString)
        }
         */

        val body = new HashMapStringAny()

        val status = httpRes.getStatus
        body.put("httpCode", status.getCode())

        if (status.getCode() != 200 && status.getCode() != 201 && !msg.parseContentOnError ) {

            if (log.isDebugEnabled()) {
                log.debug("status code={}", status.getCode())
            }

            val errorTpl = (ResultCodes.SOC_NETWORKERROR, body)

            return errorTpl
        }

        val contentTypeStr = httpRes.getHeader("Content-Type")
        val content = httpRes.getContent()

        val contentStr = content.toString(Charset.forName(msg.charSet))
        val contentType = parseContentType(contentTypeStr, contentStr)

        if (log.isDebugEnabled()) {
            log.debug("contentType={},contentStr={}", contentType, contentStr)
        }

        if (msg.pluginObj != null && msg.pluginObj.isInstanceOf[HttpResponsePlugin]) {
            val resPlugin = msg.pluginObj.asInstanceOf[HttpResponsePlugin]
            var code = resPlugin.parseContent(msg, contentStr, body)
            if (status.getCode() != 200 && status.getCode() != 201) {
                code = ResultCodes.SOC_NETWORKERROR
            }
            val succTpl = (code, body)
            return succTpl
        }

        var code = ResultCodes.SOC_NETWORKERROR
        contentType match {
            case HttpMsgDefine.MIMETYPE_JSON =>
                code = parseJsonContent(msg, contentStr, body)
            case HttpMsgDefine.MIMETYPE_XML =>
                code = parseXmlContent(msg, contentStr, body)
            case HttpMsgDefine.MIMETYPE_FORM =>
                code = parseFormContent(msg, contentStr, body)
            case _ =>
                log.error("unknown content type serviceId=%d,msgId=%d,contentType=%s".format(msg.serviceId, msg.msgId, contentType))
        }

        if (status.getCode() != 200 && status.getCode() != 201) {
            code = ResultCodes.SOC_NETWORKERROR
        }
        val succTpl = (code, body)
        succTpl
    }

    def parseContentType(contentTypeStr: String, contentStr: String): String = {

        if (contentTypeStr == null || contentTypeStr == "" || contentTypeStr.startsWith(";")) {
            if (contentStr.startsWith("{")) return HttpMsgDefine.MIMETYPE_JSON
            else if (contentStr.startsWith("[")) return HttpMsgDefine.MIMETYPE_JSON
            else if (contentStr.startsWith("<")) return HttpMsgDefine.MIMETYPE_XML
            else return HttpMsgDefine.MIMETYPE_FORM
        }

        val ss = contentTypeStr.split(";")

        ss(0).trim match {

            case HttpMsgDefine.MIMETYPE_XML2 =>
                HttpMsgDefine.MIMETYPE_XML
            case HttpMsgDefine.MIMETYPE_HTML =>
                if (contentStr.startsWith("{")) HttpMsgDefine.MIMETYPE_JSON
                else if (contentStr.startsWith("<")) HttpMsgDefine.MIMETYPE_XML
                else HttpMsgDefine.MIMETYPE_FORM
            case HttpMsgDefine.MIMETYPE_PLAIN =>
                if (contentStr.startsWith("{")) HttpMsgDefine.MIMETYPE_JSON
                else if (contentStr.startsWith("[")) return HttpMsgDefine.MIMETYPE_JSON
                else if (contentStr.startsWith("<")) HttpMsgDefine.MIMETYPE_XML
                else HttpMsgDefine.MIMETYPE_FORM
            case HttpMsgDefine.MIMETYPE_JSON2 =>
                HttpMsgDefine.MIMETYPE_JSON
            case _ =>
                ss(0)
        }
    }

    def parseJsonContent(msg: HttpMsgDefine, contentStr: String, body: HashMapStringAny): Int = {

        var code = 0

        try {

            if (!contentStr.startsWith("{")) {
                log.error("not a valid json,json=" + contentStr)
                code = ResultCodes.SERVICE_INTERNALERROR
                return code
            }

            val valueTree = mapper.readTree(contentStr)

            for (f <- msg.resFields if (f.path != null && f.path != "")) {
                val value = parseJsonValue(valueTree, f.path, f.tlvType)
                if (value != null) {
                    body.put(f.key, value)
                }
            }

            if (msg.resultCodeField != "") {
                val s = body.s(msg.resultCodeField, "")
                if (s == "")
                    code = ResultCodes.SERVICE_INTERNALERROR
                else
                    code = s.toInt
            }

        } catch {
            case e: Exception =>
                log.error("cannot parse content, content={}", contentStr, e)
                code = ResultCodes.SERVICE_INTERNALERROR
        }

        code
    }

    def parseJsonNodeToString(node: JsonNode): String = {
        return if (node.isValueNode) node.asText() else node.toString();
    }

    def parseJsonValue(valueTree: JsonNode, path: String, tlvType: TlvType): Any = {

        val ss = CachedSplitter.dotSplitter.strToArray(path)
        var i = 0
        var next = valueTree

        while (i < ss.size) {
            if (next == null) return null
            if (!next.isInstanceOf[ObjectNode]) return null
            next = next.get(ss(i))
            i += 1
        }

        if (next == null) return null

        if (TlvType.isSimple(tlvType.cls))
            return parseJsonNodeToString(next)

        if (tlvType.cls == TlvType.CLS_STRUCT) {
            if (!next.isInstanceOf[ObjectNode]) return null
            val objNode = next.asInstanceOf[ObjectNode]
            val map = new HashMapStringAny()
            for (f <- tlvType.structDef.fields) {
                val key = f.name
                val node = objNode.get(key)
                if (node == null) {
                    map.put(key, null)
                } else {
                    val s = parseJsonNodeToString(node)
                    map.put(key, s)
                }
            }
            return map
        }
        
        if (tlvType.cls == TlvType.CLS_OBJECT) {
            if (!next.isInstanceOf[ObjectNode]) return null
            val objNode = next.asInstanceOf[ObjectNode]
            val map = new HashMapStringAny()
            for (f <- tlvType.objectDef.fields) {
                val type_name = f.name
                val key = tlvType.objectDef.typeToKeyMap.getOrElse(type_name,null)
                val node = objNode.get(key)
                if (node == null) {
                    map.put(key, null)
                } else {
                    val s = parseJsonNodeToString(node)
                    map.put(key, s)
                }
            }
            return map
        }

        if (tlvType.cls == TlvType.CLS_STRINGARRAY || tlvType.cls == TlvType.CLS_INTARRAY || tlvType.cls == TlvType.CLS_LONGARRAY || tlvType.cls == TlvType.CLS_DOUBLEARRAY) {
            if (!next.isInstanceOf[ArrayNode]) return null
            val arrayNode = next.asInstanceOf[ArrayNode]
            val buff = new ArrayBufferString()
            val it = arrayNode.elements()
            while (it.hasNext()) {
                val tt = it.next()
                buff += parseJsonNodeToString(tt)
            }
            return buff
        }

        if (tlvType.cls == TlvType.CLS_STRUCTARRAY) {
            if (!next.isInstanceOf[ArrayNode]) return null
            val arrayNode = next.asInstanceOf[ArrayNode]
            val buff = new ArrayBufferMap()
            val it = arrayNode.elements()
            while (it.hasNext()) {
                val tt = it.next()

                if (!tt.isInstanceOf[ObjectNode]) return null
                val objNode = tt.asInstanceOf[ObjectNode]
                val map = new HashMapStringAny()
                for (f <- tlvType.structDef.fields) {
                    val key = f.name
                    val node = objNode.get(key)
                    if (node == null) {
                        map.put(key, null)
                    } else {
                        val s = parseJsonNodeToString(node)
                        map.put(key, s)
                    }
                }

                buff += map
            }
            return buff
        }

        if (tlvType.cls == TlvType.CLS_OBJECTARRAY) {
            if (!next.isInstanceOf[ArrayNode]) return null
            val arrayNode = next.asInstanceOf[ArrayNode]
            val buff = new ArrayBufferMap()
            val it = arrayNode.elements()
            while (it.hasNext()) {
                val tt = it.next()

                if (!tt.isInstanceOf[ObjectNode]) return null
                val objNode = tt.asInstanceOf[ObjectNode]
                val map = new HashMapStringAny()
                for (f <- tlvType.objectDef.fields) {
                    val type_name = f.name
                    val key = tlvType.objectDef.typeToKeyMap.getOrElse(type_name,null)
                    val node = objNode.get(key)
                    if (node == null) {
                        map.put(key, null)
                    } else {
                        val s = parseJsonNodeToString(node)
                        map.put(key, s)
                    }
                }

                buff += map
            }
            return buff
        }
        
        return null
    }

    def parseFormContent(msg: HttpMsgDefine, contentStr: String, body: HashMapStringAny): Int = {

        val map = new HashMapStringString()

        val ss = contentStr.split("&")

        for (s <- ss) {
            val tt = s.split("=")
            if (tt.size >= 2) {
                val value = URLDecoder.decode(tt(1), msg.charSet)
                map.put(tt(0), value)
            }
        }

        var code = 0

        for (f <- msg.resFields) {
            val value = map.getOrElse(f.path, null)
            if (value != null) {
                body.put(f.key, value)
            }
        }

        if (msg.resultCodeField != "") {
            val s = body.s(msg.resultCodeField, "")
            if (s == "")
                code = ResultCodes.SERVICE_INTERNALERROR
            else
                code = s.toInt
        }

        code
    }

    def removeNs(contentStr: String): String = {
        val s1 = r1.replaceAllIn(contentStr, "<")
        val s2 = r2.replaceAllIn(s1, "</")
        r2.replaceAllIn(s2, " ")
    }

    // TODO support map of struct, array string, array int
    def parseXmlContent(msg: HttpMsgDefine, contentStr: String, body: HashMapStringAny): Int = {
        var code = 0

        try {

            val contentStrRemovedNs = removeNs(contentStr)
            val rootNode = XML.loadString(contentStrRemovedNs)

            for (f <- msg.resFields) {
                val value = parseXmlValue(rootNode, f.path)
                if (value != null) {
                    body.put(f.key, value)
                }
            }

            if (msg.resultCodeField != "") {
                val s = body.s(msg.resultCodeField, "")
                if (s == "")
                    code = ResultCodes.SERVICE_INTERNALERROR
                else
                    code = s.toInt
            }

        } catch {
            case e: Exception =>
                code = ResultCodes.SERVICE_INTERNALERROR
                log.error("cannot parse content, content={}", contentStr)
        }

        code
    }

    def parseXmlValue(rootNode: Elem, path: String): Any = {

        val ss = CachedSplitter.dotSplitter.strToArray(path)
        var i = 0
        var next = rootNode
        while (i < ss.size) {
            if (next == null) return null

            if (ss(i).indexOf("@") > 0) {

                val sss = ss(i).split("@")

                val t = next \ sss(0)
                if (t == null) return null

                return (t \ ("@" + sss(1))).toString

            } else {
                val t = next \ ss(i)
                next = if (t != null && t.size > 0) t.head.asInstanceOf[Elem] else null
            }

            i += 1
        }
        if (next == null) return null
        next.text

    }

    def receive(v: Any): Unit = {

        v match {

            case HttpClientResponse(sequence, httpRes) =>

                val saved = dataMap.remove(sequence)
                if (saved != null) {
                    saved.data match {

                        case req: Request =>

                            val msg = findMsg(req.serviceId, req.msgId)
                            if (msg != null) {

                                val (code, body) = generateResponseBody(msg, httpRes)

                                val res = new Response(code, body, req)
                                res.remoteAddr = msg.host

                                receiver_f(new RequestResponseInfo(req, res))
                            }

                        case req: SocRequest =>

                            val msg = findMsg(req.serviceId, req.msgId)
                            if (msg != null) {

                                val (code, body) = generateResponseBody(msg, httpRes)
                                val res = new SocResponse(req.requestId, code, body)
                                res.remoteAddr = msg.host

                                receiver_f(new SocRequestResponseInfo(req, res))
                            }
                    }
                } else {
                    log.warn("receive but sequence not found, seq={}", sequence)
                }

            case HttpClientTimeout(sequence) =>
                val saved = dataMap.remove(sequence)
                if (saved != null) {
                    saved.data match {

                        case req: Request =>
                            val res = createErrorResponse(ResultCodes.SOC_TIMEOUT, req)
                            val msg = findMsg(req.serviceId, req.msgId)
                            if (msg != null) {
                                res.remoteAddr = msg.host
                            }
                            receiver_f(new RequestResponseInfo(req, res))
                        case req: SocRequest =>
                            val res = createErrorResponse(ResultCodes.SOC_TIMEOUT, req)
                            val msg = findMsg(req.serviceId, req.msgId)
                            if (msg != null) {
                                res.remoteAddr = msg.host
                            }
                            receiver_f(new SocRequestResponseInfo(req, res))

                    }
                } else {
                    log.error("timeout but sequence not found, seq={}", sequence)
                }
            case HttpClientNetworkError(sequence) =>
                val saved = dataMap.remove(sequence)
                if (saved != null) {

                    saved.sendTimes += 1
                    val now = System.currentTimeMillis
                    if (saved.sendTimes >= retryTimes || now + 30 >= saved.sendTime + saved.timeout) {

                        saved.data match {

                            case req: Request =>
                                val res = createErrorResponse(ResultCodes.SOC_NETWORKERROR, req)
                                val msg = findMsg(req.serviceId, req.msgId)
                                if (msg != null) {
                                    res.remoteAddr = msg.host
                                }
                                receiver_f(new RequestResponseInfo(req, res))
                            case req: SocRequest =>
                                val res = createErrorResponse(ResultCodes.SOC_NETWORKERROR, req)
                                val msg = findMsg(req.serviceId, req.msgId)
                                if (msg != null) {
                                    res.remoteAddr = msg.host
                                }
                                receiver_f(new SocRequestResponseInfo(req, res))
                        }
                    } else {

                        log.warn("resend data, req={},sendTimes={}", saved.data, saved.sendTimes)

                        saved.data match {

                            case req: Request =>
                                send(req, saved.timeout)
                            case req: SocRequest =>
                                send(req, saved.timeout)
                        }

                    }
                } else {
                    log.error("network error but sequence not found, seq={}", sequence)
                }
        }
    }

}

