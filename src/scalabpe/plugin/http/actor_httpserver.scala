package scalabpe.plugin.http

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.Charset
import java.text.ParsePosition
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.GregorianCalendar
import java.util.Locale
import java.util.TimeZone
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.matching.Regex
import scala.xml.Node

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.Cookie
import org.jboss.netty.handler.codec.http.CookieDecoder
import org.jboss.netty.handler.codec.http.CookieEncoder
import org.jboss.netty.handler.codec.http.DefaultCookie
import org.jboss.netty.handler.codec.http.DefaultHttpResponse
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.handler.codec.http.HttpMethod
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.jboss.netty.handler.codec.http.HttpVersion

import scalabpe.core.Actor
import scalabpe.core.AfterInit
import scalabpe.core.ArrayBufferAny
import scalabpe.core.ArrayBufferInt
import scalabpe.core.ArrayBufferLong
import scalabpe.core.ArrayBufferDouble
import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.Xhead
import scalabpe.core.BeforeClose
import scalabpe.core.Closable
import scalabpe.core.CryptHelper
import scalabpe.core.Dumpable
import scalabpe.core.Flow
import scalabpe.core.HashMapStringAny
import scalabpe.core.HashMapStringString
import scalabpe.core.HttpSosRequest
import scalabpe.core.HttpSosRequestResponseInfo
import scalabpe.core.HttpSosResponse
import scalabpe.core.InvokeResult
import scalabpe.core.IpUtils
import scalabpe.core.JsonCodec
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.QuickTimer
import scalabpe.core.QuickTimerEngine
import scalabpe.core.Refreshable
import scalabpe.core.Request
import scalabpe.core.ResultCodes
import scalabpe.core.Router
import scalabpe.core.TlvType
import scalabpe.core.TlvCodec
import scalabpe.core.TlvCodecs
import scalabpe.core.TypeSafe

object HttpTimeHelper {

    val FORMAT_TYPE_TIMESTAMP = "yyyy-MM-dd HH:mm:ss"

    def convertTimestamp(s: String): Date = {
        if (s == null || s == "") {
            return null;
        }

        var str = s
        if (str.length() == 10) {
            str += " 00:00:00";
        }

        return new SimpleDateFormat(FORMAT_TYPE_TIMESTAMP).parse(str, new ParsePosition(0));
    }

    def convertTimestamp(d: Date): String = {
        if (d == null) {
            return null;
        }
        return new SimpleDateFormat(FORMAT_TYPE_TIMESTAMP).format(d);
    }

}

class HttpServerCacheData(val req: HttpSosRequest, val timer: QuickTimer) {
    val sendTime = System.currentTimeMillis
}

class HttpServerRequest(val httpReq: HttpRequest, val connId: String)
class HttpServerRequestTimeout(val requestId: String)

class OsapKeyData(val key: String, val st: Long, val et: Long)

class OsapMerchantCfg(val merchantName: String, val appId: Int, val areaId: Int,
                      val md5Key: ArrayBuffer[OsapKeyData] = ArrayBuffer[OsapKeyData](),
                      val ipSet: HashSet[String] = HashSet[String](),
                      val privilegeSet: HashSet[String] = HashSet[String]())

class HttpServerActor(val router: Router, val cfgNode: Node) extends Actor
        with BeforeClose  with AfterInit with HttpServer4Netty with Logging with Dumpable with Closable { // with Refreshable

    type ArrayBufferPattern = ArrayBuffer[Tuple2[Int, String]];

    val MIMETYPE_FORM = "application/x-www-form-urlencoded"
    val MIMETYPE_JSON = "application/json"
    val MIMETYPE_JAVASCRIPT = "text/javascript"
    val MIMETYPE_XML = "text/xml"
    val MIMETYPE_XML2 = "application/xml"
    val MIMETYPE_HTML = "text/html"
    val MIMETYPE_PLAIN = "text/plain"
    val MIMETYPE_DEFAULT = "application/octet-stream"
    val MIMETYPE_MULTIPART = "multipart/form-data"

    val EMPTY_STRINGMAP = new HashMapStringString()

    val errorFormat = """{"return_code":%d,"return_message":"%s","data":{}}"""
    val errorFormatCodeMessageNoData = """{"code":%d,"message":"%s"}"""

    val HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    val HTTP_DATE_GMT_TIMEZONE = "GMT";

    val df_tl = new ThreadLocal[SimpleDateFormat]() {
        override def initialValue(): SimpleDateFormat = {
            val df = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US)
            df.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));
            df
        }
    }

    var threadNum = 4
    val queueSize = 20000

    var host: String = "*"
    var port: Int = 0
    var timeout = 30000
    var idleTimeout = 45000
    var timerInterval: Int = 50
    
    var removeReturnMessageInBody = false
    var jsonStyle = ""
    var returnMessageFieldNames = ArrayBufferString("return_message", "resultMsg", "failReason", "resultMessage", "result_msg", "fail_reason", "result_message")
    
    var defaultVerify = "false"
    var sessionFieldName = "jsessionId"
    var sessionCookieName = "JSESSIONID"
    var sessionMode = 1 // 1=auto 2=manual
    var sessionIpBind = false
    var sessionHttpOnly = true
    var sessionMaxAge = -1
    var sessionPath = ""
    var jsonpName = "jsonp"
    var logUserAgent = false
    var redirect302FieldName = "redirectUrl302"
    var requestUriFieldName = "requestUri"
    var domainNameFieldName = "domainName"
    var queryStringFieldName = "queryString"
    var contentTypeFieldName = "contentType"
    var contentDataFieldName = "contentData"
    var maxContentLength = 5000000
    var maxInitialLineLength = 16000
    var maxHeaderSize = 16000
    var maxChunkSize = 16000
    var uploadDir = "webapp/upload"
    var maxUploadLength = 5000000
    var sessionEncKey = CryptHelper.toHexString("9*cbd35w".getBytes());
    var accessControlAllowOriginGlobal = ""

    val classexMap = new HashMap[String, HashMap[String, String]]()
    val urlMapping = new HashMap[String, Tuple4[Int, Int, String, ArrayBufferPattern]]
    val msgAttrs = new HashMap[String, String]
    val whiteIpSet = new HashMap[String, HashSet[String]]
    val msgPlugins = new HashMap[String, HttpServerPlugin]
    val cookieMap = new HashMap[String, ArrayBuffer[Tuple3[String, String, String]]]()
    val headerMap = new HashMap[String, ArrayBuffer[Tuple2[String, String]]]()
    val logFilterMap = new HashMap[String, ArrayBuffer[Regex]]()

    val webappDir = Flow.router.rootDir + File.separator + "webapp"
    val webappDirExisted = new File(webappDir).exists()
    val webappStaticDir = webappDir + File.separator + "static"
    val webappStaticDirExisted = new File(webappStaticDir).exists()

    var defaultMimeTypes =
        """
text/html html htm
text/plain txt text
text/css css
text/javascript js
text/xml xml
application/json json
image/gif gif
image/jpeg jpeg jpg jpe
image/tiff tiff tif
image/png png
image/x-icon ico
application/x-rar-compressed rar
application/zip zip
application/x-7z-compressed 7z
application/x-gzip gz
    """
    var mimeTypeMap = new HashMapStringString

    var devMode = false
    var cacheEnabled = true
    var cacheFiles = "html htm css js"
    var cacheFileSize = 25000
    var cacheFilesSet = new HashSet[String]
    val cacheMap = new ConcurrentHashMap[String, Array[Byte]]()
    var httpCacheSeconds = 24 * 3600
    var skipMinFile = false
    var urlArgs = "?"
    var hasPattern = false

    var codecs: TlvCodecs = _
    val localIp = IpUtils.localIp()
    var threadFactory: ThreadFactory = _
    var pool: ThreadPoolExecutor = _
    var qte: QuickTimerEngine = null

    val merchantMap = new AtomicReference[HashMap[String, OsapMerchantCfg]]() // 可由外部系统来初始化此数据来增加权限功能, 插件本身不提供权限加载机制(功能比较鸡肋，已删除)

    var nettyHttpServer: NettyHttpServer = _

    val sequence = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[String, HttpServerCacheData]()

    var timer: Timer = _

    init

    def init() {

        codecs = router.codecs
        merchantMap.set(new HashMap[String, OsapMerchantCfg]())

        var s = (cfgNode \ "@threadNum").text
        if (s != "") threadNum = s.toInt

        s = (cfgNode \ "@host").text
        if (s != "") host = s

        s = (cfgNode \ "@port").text
        if (s != "") port = s.toInt

        s = (cfgNode \ "@timeout").text
        if (s != "") timeout = s.toInt

        s = (cfgNode \ "@idleTimeout").text
        if (s != "") idleTimeout = s.toInt

        s = (cfgNode \ "@timerInterval").text
        if (s != "") timerInterval = s.toInt

        s = (cfgNode \ "@defaultVerify").text
        if (s != "") defaultVerify = s

        s = (cfgNode \ "@returnMessageFieldNames").text
        if (s != "") {
            returnMessageFieldNames = new ArrayBufferString()
            val ss = s.split(",")
            for (s <- ss) returnMessageFieldNames += s
        }

        s = (cfgNode \ "@removeReturnMessageInBody").text
        if (s != "") {
            removeReturnMessageInBody = TypeSafe.isTrue(s)
        }
        s = (cfgNode \ "@jsonStyle").text
        if (s != "") jsonStyle = s

        s = (cfgNode \ "@sessionFieldName").text
        if (s != "") sessionFieldName = s

        s = (cfgNode \ "@sessionCookieName").text
        if (s != "") sessionCookieName = s

        s = (cfgNode \ "@sessionMode").text
        if (s != "") sessionMode = s.toInt

        s = (cfgNode \ "@sessionIpBind").text
        if (s != "") sessionIpBind = TypeSafe.isTrue(s)

        s = (cfgNode \ "@sessionHttpOnly").text
        if (s != "") sessionHttpOnly = TypeSafe.isTrue(s)

        s = (cfgNode \ "@sessionMaxAge").text
        if (s != "") sessionMaxAge = s.toInt

        s = (cfgNode \ "@sessionPath").text
        if (s != "") sessionPath = s

        s = (cfgNode \ "@sessionEncKey").text
        if (s != "") sessionEncKey = CryptHelper.toHexString(s.getBytes());

        s = (cfgNode \ "@maxContentLength").text
        if (s != "") maxContentLength = s.toInt
        s = (cfgNode \ "@uploadDir").text
        if (s != "") uploadDir = s
        s = (cfgNode \ "@maxUploadLength").text
        if (s != "") maxUploadLength = s.toInt
        s = (cfgNode \ "@maxInitialLineLength").text
        if (s != "") maxInitialLineLength = s.toInt
        s = (cfgNode \ "@maxHeaderSize").text
        if (s != "") maxHeaderSize = s.toInt
        s = (cfgNode \ "@maxChunkSize").text
        if (s != "") maxChunkSize = s.toInt

        val mimeItemlist = (cfgNode \ "MimeTypes" \ "Item").toList
        for (item <- mimeItemlist) {
            var s = item.text.toString.trim
            defaultMimeTypes += "\n" + s
        }
        val types = defaultMimeTypes.split("\n").map(_.trim).filter(_ != "")
        for (s <- types) {
            val ss = s.split(" ")
            if (ss.length >= 2) {
                val tp = ss(0)
                for (i <- 1 until ss.length)
                    mimeTypeMap.put(ss(i), tp)
            }
        }
        log.debug("http mimetypes:\n" + mimeTypeMap.mkString("\n"))

        s = (cfgNode \ "@cacheEnabled").text
        if (s != "") cacheEnabled = TypeSafe.isTrue(s)

        s = (cfgNode \ "@cacheFileSize").text
        if (s != "") cacheFileSize = s.toInt

        s = (cfgNode \ "@cacheFiles").text
        if (s != "") { cacheFiles += " " + s }
        cacheFiles.split(" ").filter(_ != "").foreach(cacheFilesSet.add(_))
        log.debug("http cache files:" + cacheFilesSet.mkString(","))

        s = (cfgNode \ "@httpCacheSeconds").text
        if (s != "") httpCacheSeconds = s.toInt

        s = (cfgNode \ "@httpTemplateCache").text
        if (s != "")
            router.parameters.put("httpserver.template.cache", s) // used by plugin

        s = (cfgNode \ "@httpTemplateCheckInterval").text
        if (s != "")
            router.parameters.put("httpserver.template.checkInterval", s) // used by plugin

        s = (cfgNode \ "@urlArgs").text
        if (s != "") urlArgs = s

        s = (cfgNode \ "@skipMinFile").text
        if (s != "") skipMinFile = TypeSafe.isTrue(s)

        s = (cfgNode \ "@devMode").text
        if (s != "") devMode = TypeSafe.isTrue(s)

        s = (cfgNode \ "@logUserAgent").text
        if (s != "") logUserAgent = TypeSafe.isTrue(s)

        s = (cfgNode \ "@accessControlAllowOrigin").text
        if (s != "") accessControlAllowOriginGlobal = s

        if (devMode) {
            cacheEnabled = false
            httpCacheSeconds = 0
            router.parameters.put("httpserver.template.cache", "0")
            skipMinFile = true
        }

        val itemlist = (cfgNode \ "UrlMapping" \ "Item").toList
        for (item <- itemlist) {
            var s = item.text.toString.trim
            val ss = s.split(",")

            var url = ss(0).trim.toLowerCase
            if (url.startsWith("/")) url = url.substring(1)
            val serviceId = ss(1).toInt
            val msgId = ss(2).toInt
            var pattern = new ArrayBufferPattern()
            val oldUrl = url
            if (url.indexOf(":") >= 0) { // restful parameters
                val ss = url.split("/")
                for (i <- 0 until ss.length) {
                    if (ss(i).startsWith(":") && ss(i).length >= 2) {
                        pattern += new Tuple2(i, ss(i).substring(1))
                        ss(i) = "*"
                        hasPattern = true
                    }
                }
                url = ss.mkString("/")
            }
            urlMapping.put(url, (serviceId, msgId, oldUrl, pattern))

            s = (item \ "@charset").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-charset", s)
            s = (item \ "@requestContentType").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-requestContentType", s)
            s = (item \ "@responseContentType").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-responseContentType", s)
            s = (item \ "@caseInsensitive").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-caseInsensitive", s)
            s = (item \ "@verify").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-verify", s)

            s = (item \ "@bodyOnly").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-bodyOnly", s)

            s = (item \ "@encodeRequest").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-encodeRequest", s)

            s = (item \ "@accessControlAllowOrigin").text
            if (s != "")
                msgAttrs.put(serviceId + "-" + msgId + "-accessControlAllowOrigin", s)

            s = (item \ "@whiteIps").text
            if (s != "") {
                val set = HashSet[String]()
                val ss = s.split(",")
                for (ts <- ss) {
                    val ipgrp = router.getConfig(ts)
                    if (ipgrp != "") {
                        val ss2 = ipgrp.split(",")
                        for (ts2 <- ss2) set.add(ts2)
                    } else {
                        set.add(ts)
                    }
                }
                whiteIpSet.put(serviceId + "-" + msgId, set)
            }

            s = (item \ "@logFilters").text
            if (s != "") {
                val buff = ArrayBuffer[Regex]()
                val ss = s.split(",")
                for (ts <- ss) {
                    val grp = router.getConfig(ts)
                    if (grp != "") {
                        buff += grp.r
                    } else {
                        buff += ts.r
                    }
                }
                logFilterMap.put(serviceId + "-" + msgId, buff)
            }

            s = (item \ "@plugin").text
            if (s != "") {

                var pluginParam = ""
                val p = s.indexOf(":")
                if (p >= 0) {
                    pluginParam = s.substring(p + 1)
                    s = s.substring(0, p)
                }

                if (s == "plain") s = "scalabpe.plugin.http.PlainTextPlugin"
                if (s == "redirect") s = "scalabpe.plugin.http.RedirectPlugin"
                if (s == "download") s = "scalabpe.plugin.http.DownloadPlugin"
                if (s == "static_file") s = "scalabpe.plugin.http.StaticFilePlugin"
                if (s == "template") s = "scalabpe.plugin.http.TemplatePlugin"

                if (s.indexOf(".") < 0) s = "scalabpe.flow." + s

                msgAttrs.put(serviceId + "-" + msgId + "-plugin", s)
                if (pluginParam != "")
                    msgAttrs.put(serviceId + "-" + msgId + "-pluginParam", pluginParam)

                var plugin = s
                try {

                    val obj = Class.forName(plugin).getConstructors()(0).newInstance()
                    if (!obj.isInstanceOf[HttpServerPlugin]) {
                        throw new RuntimeException("plugin %s is not HttpServerPlugin".format(plugin))
                    }
                    val pluginObj = obj.asInstanceOf[HttpServerPlugin]
                    msgPlugins.put(serviceId + "-" + msgId, pluginObj)

                } catch {
                    case e: Exception =>
                        log.error("plugin {} cannot be loaded", plugin)
                        throw e
                }

            }

            val tlvCodec = router.findTlvCodec(serviceId)
            if (tlvCodec != null) {
                val map = HashMap[String, String]()
                val keyMap = tlvCodec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP)

                for ((key, typeName) <- keyMap) {
                    val classex = tlvCodec.codecAttributes.getOrElse("classex-" + typeName, null)
                    if (classex != null) map.put(key, classex)
                    val tlvType = tlvCodec.typeNameToCodeMap.getOrElse(typeName, null)
                    if (tlvType.cls == TlvType.CLS_STRUCT) {
                        for (f <- tlvType.structDef.fields) {
                            val n = f.name
                            val classex2 = tlvCodec.codecAttributes.getOrElse("classex-" + tlvType.name + "-" + n, null)
                            if (classex2 != null) map.put(key + "-" + n, classex2)
                        }
                    }
                    if (tlvType.cls == TlvType.CLS_STRUCTARRAY) {
                        for (f <- tlvType.structDef.fields) {
                            val n = f.name
                            val classex2 = tlvCodec.codecAttributes.getOrElse("classex-" + tlvType.itemType.name + "-" + n, null)
                            if (classex2 != null) map.put(key + "-" + n, classex2)
                        }
                    }
                    if (tlvType.cls == TlvType.CLS_OBJECT) {
                        for (f <- tlvType.objectDef.fields) {
                            val type_name = f.name
                            val n = tlvType.objectDef.typeToKeyMap.getOrElse(type_name,null)
                            val classex2 = tlvCodec.codecAttributes.getOrElse("classex-" + tlvType.name + "-" + n, null)
                            if (classex2 != null) map.put(key + "-" + n, classex2)
                        }
                    }
                    if (tlvType.cls == TlvType.CLS_OBJECTARRAY) {
                        for (f <- tlvType.objectDef.fields) {
                            val type_name = f.name
                            val n = tlvType.objectDef.typeToKeyMap.getOrElse(type_name,null)
                            val classex2 = tlvCodec.codecAttributes.getOrElse("classex-" + tlvType.itemType.name + "-" + n, null)
                            if (classex2 != null) map.put(key + "-" + n, classex2)
                        }
                    }
                }
                classexMap.put(serviceId + "-" + msgId, map)
            }

            if (tlvCodec != null) {

                val keyMap1 = tlvCodec.msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP)
                if (keyMap1.contains(sessionFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-sessionId-req", "1")

                val keyMap2 = tlvCodec.msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP)
                if (keyMap2.contains(sessionFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-sessionId-res", "1")

                if (keyMap1.contains(requestUriFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-" + requestUriFieldName, "1")
                if (keyMap1.contains(domainNameFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-" + domainNameFieldName, "1")
                if (keyMap1.contains(queryStringFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-" + queryStringFieldName, "1")
                if (keyMap1.contains(contentTypeFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-" + contentTypeFieldName, "1")
                if (keyMap1.contains(contentDataFieldName))
                    msgAttrs.put(serviceId + "-" + msgId + "-" + contentDataFieldName, "1")

                val attributes = tlvCodec.msgAttributes.getOrElse(msgId, null)

                val cookiebuff1 = ArrayBuffer[Tuple3[String, String, String]]()
                for ((key, dummy) <- keyMap1 if key != sessionFieldName) {
                    val s = attributes.getOrElse("req-" + key + "-cookieName", "")
                    if (s != "")
                        cookiebuff1 += new Tuple3[String, String, String](key, s, null)
                }
                if (cookiebuff1.size > 0) cookieMap.put(serviceId + "-" + msgId + "-req", cookiebuff1)

                val cookiebuff2 = ArrayBuffer[Tuple3[String, String, String]]()
                for ((key, dummy) <- keyMap2 if key != sessionFieldName) {
                    val s = attributes.getOrElse("res-" + key + "-cookieName", "")
                    val opt = attributes.getOrElse("res-" + key + "-cookieOption", "")
                    if (s != "")
                        cookiebuff2 += new Tuple3[String, String, String](key, s, opt)
                }
                if (cookiebuff2.size > 0) cookieMap.put(serviceId + "-" + msgId + "-res", cookiebuff2)

                val headerbuff1 = ArrayBuffer[Tuple2[String, String]]()
                for ((key, dummy) <- keyMap1) {
                    val s = attributes.getOrElse("req-" + key + "-headerName", "")
                    if (s != "")
                        headerbuff1 += new Tuple2[String, String](key, s)
                }
                if (headerbuff1.size > 0) headerMap.put(serviceId + "-" + msgId + "-req", headerbuff1)

                val headerbuff2 = ArrayBuffer[Tuple2[String, String]]()
                for ((key, dummy) <- keyMap2) {
                    val s = attributes.getOrElse("res-" + key + "-headerName", "")
                    if (s != "")
                        headerbuff2 += new Tuple2[String, String](key, s)
                }
                if (headerbuff2.size > 0) headerMap.put(serviceId + "-" + msgId + "-res", headerbuff2)

            }

        }

        if (Router.testMode) return

        nettyHttpServer = new NettyHttpServer(this, port, host, idleTimeout,
            maxContentLength, maxInitialLineLength, maxHeaderSize, maxChunkSize,
            uploadDir, maxUploadLength)

        threadFactory = new NamedThreadFactory("httpserver")
        pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), threadFactory)
        pool.prestartAllCoreThreads()

        qte = new QuickTimerEngine(onTimeout, timerInterval)
    }

    def close() {

        if (Router.testMode) return

        if (timer != null) {
            timer.cancel()
            timer = null
        }

        if (pool != null) {
            val t1 = System.currentTimeMillis
            pool.shutdown()
            pool.awaitTermination(5, TimeUnit.SECONDS)
            val t2 = System.currentTimeMillis
            if (t2 - t1 > 100)
                log.warn("long time to close httpserver threadpool, ts={}", t2 - t1)
        }

        nettyHttpServer.close()

        if (qte != null) {
            qte.close()
            qte = null
        }

        log.info("httpserver stopped")
    }

    def afterInit() {

        if (Router.testMode) return

        nettyHttpServer.start()
        log.info("netty httpserver started port(" + port + ")")
    }

    def beforeClose() {
        if (Router.testMode) return
        nettyHttpServer.closeReadChannel()
    }

    def stats(): Array[Int] = {
        if (Router.testMode) return new Array[Int](0)

        nettyHttpServer.stats
    }

    def dump() {
        if (Router.testMode) return

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")
        buff.append("merchantMap.size=").append(merchantMap.get.size).append(",")
        buff.append("dataMap.size=").append(dataMap.size).append(",")

        log.info(buff.toString)

        nettyHttpServer.dump

        qte.dump()
    }

    def receive(req: HttpRequest, connId: String) {
        receive(new HttpServerRequest(req, connId))
    }

    def onTimeout(data: Any): Unit = {
        val requestId = data.asInstanceOf[String]
        receive(new HttpServerRequestTimeout(requestId))
    }

    override def receive(v: Any) {

        try {
            pool.execute(new Runnable() {
                def run() {
                    try {
                        onReceive(v)
                    } catch {
                        case e: Exception =>
                            log.error("httpserver receive exception v={}", v, e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                if (v.isInstanceOf[HttpServerRequest]) {
                    val r = v.asInstanceOf[HttpServerRequest]
                    val requestId = uuid()
                    replyError(r.httpReq, r.connId, requestId, ResultCodes.SERVICE_FULL)
                }
                log.error("httpserver queue is full")
        }

    }

    def onReceive(v: Any): Unit = {

        v match {

            case req: HttpServerRequest =>

                processRequest(req.httpReq, req.connId)

            case d: HttpServerRequestTimeout =>

                val data = dataMap.remove(d.requestId)
                if (data == null) {
                    return
                }
                reply(data.req, ResultCodes.SERVICE_TIMEOUT, HashMapStringAny())

            case res: InvokeResult =>

                val requestId = res.requestId
                val data = dataMap.remove(requestId)
                if (data == null) {
                    return
                }
                data.timer.cancel()
                reply(data.req, res.code, res.res)

            case _ =>

                log.error("unknown msg received")
        }
    }

    def processRequest(httpReq: HttpRequest, connId: String) {

        val method = httpReq.getMethod()
        if (method != HttpMethod.POST && method != HttpMethod.GET && method != HttpMethod.HEAD) {
            val requestId = uuid()
            replyError(httpReq, connId, requestId, -10250013)
            return
        }

        val requestId = uuid()
        val xhead = parseXhead(httpReq, connId, requestId)

        var uri = httpReq.getUri()
        processRequest(httpReq, connId, xhead, requestId, uri)
    }
    
    def processRequest(httpReq: HttpRequest, connId: String, xhead: HashMapStringAny, requestId: String, uri: String) {

        val (serviceId, msgId, mappingUri, patterns) = mappingUrl(uri)
        if (serviceId == 0) { // wrong

            val file = mappingStaticUrl(uri)
            if (file != "") {
                writeStaticFile(httpReq, connId, xhead, requestId, uri, new File(file))
                return
            }

            if (webappStaticDirExisted) {
                write404(httpReq, connId, xhead, requestId, uri)
                return
            }

            replyError(httpReq, connId, requestId, ResultCodes.SERVICE_NOT_FOUND, uri)
            return
        }

        val pluginObj = msgPlugins.getOrElse(serviceId + "-" + msgId, null)
        val clientIpPort = xhead.ns(Xhead.KEY_FIRST_ADDR)
        val clientIp = clientIpPort.substring(0, clientIpPort.indexOf(":"))
        val remoteIpPort = parseRemoteIp(connId)
        val serverIpPort = localIp + ":" + port
        var host = httpReq.getHeader("Host")
        if (host == null || host == "") host = "unknown_host"

        val (body, sessionIdChanged) = parseBody(requestId, serviceId, msgId, httpReq, host, clientIp)

        if (patterns != null && patterns.size > 0) { // parse restful parameters
            val p = uri.indexOf("?")
            var path = if (p < 0) uri else uri.substring(0, p)
            if (path.startsWith("/")) {
                path = path.substring(1)
            }
            val ss = path.split("/")
            for ((i, name) <- patterns if i >= 0 && i < ss.length) {
                body.put(name, ss(i))
            }
        }

        val whiteIps = whiteIpSet.getOrElse(serviceId + "-" + msgId, null)
        if (whiteIps != null && whiteIps.size > 0) {
            if (!whiteIps.contains(clientIp)) {
                log.error("verify failed, not in white ips, uri=" + uri)
                replyError(httpReq, connId, requestId, -10250016, uri, serviceId, msgId)
                return
            }
        }

        val verify = TypeSafe.isTrue(msgAttrs.getOrElse(serviceId + "-" + msgId + "-verify", defaultVerify))
        if (verify) {

            if (pluginObj != null && pluginObj.isInstanceOf[HttpServerVerifyPlugin]) {
                val verifyOk = pluginObj.asInstanceOf[HttpServerVerifyPlugin].verify(serviceId, msgId, xhead, body, httpReq)
                if (!verifyOk) {
                    replyError(httpReq, connId, requestId, -10250016, uri, serviceId, msgId)
                    return
                }
            } else {
                val (ok, merchantName) = getMerchantInfo(body)
                if (!ok) {
                    log.error("verify failed, merchant name parameter not found, uri=" + uri)
                    replyError(httpReq, connId, requestId, -10250016, uri, serviceId, msgId)
                    return
                }
                val cfg = merchantMap.get().getOrElse(merchantName, null)
                if (cfg == null) {
                    log.error("verify failed, merchant cfg not found, uri=" + uri)
                    replyError(httpReq, connId, requestId, -10250016,  uri, serviceId, msgId)
                    return
                }

                val verifyOk = standardVerify(serviceId, msgId, xhead, body, cfg, uri)
                if (!verifyOk) {
                    replyError(httpReq, connId, requestId, -10250016,  uri, serviceId, msgId)
                    return
                }

            }
        }

        if (pluginObj != null && pluginObj.isInstanceOf[HttpServerRequestPostParsePlugin])
            pluginObj.asInstanceOf[HttpServerRequestPostParsePlugin].afterParse(serviceId, msgId, xhead, body)

        val caseInsensitive = TypeSafe.isTrue(msgAttrs.getOrElse(serviceId + "-" + msgId + "-caseInsensitive", "0"))
        val bodyIncase = if (caseInsensitive) convertBodyCaseInsensitive(serviceId, msgId, body) else body

        val encodeRequestFlag = TypeSafe.isTrue(msgAttrs.getOrElse(serviceId + "-" + msgId + "-encodeRequest", "1"))
        val (bodyReal, ec) = if (encodeRequestFlag) router.encodeRequest(serviceId, msgId, bodyIncase) else (bodyIncase, 0)

        if (ec != 0) {
            replyError(httpReq, connId, requestId, ec, uri, serviceId, msgId)
            return
        }

        val req = new Request(
            requestId,
            connId,
            sequence.getAndIncrement(),
            1, // utf-8
            serviceId,
            msgId,
            xhead,
            bodyReal,
            this)

        val t = qte.newTimer(timeout, requestId)

        var params = "/" + mappingUri + "?" + bodyToParams(body)

        val method = httpReq.getMethod()
        if( method != HttpMethod.GET && method != HttpMethod.HEAD ) {
            val p = mappingUri.indexOf("?")
            if( p >= 0) {
                params = mappingUri.substring(0,p) + "?" + bodyToParams(body)
            } else {
                params = mappingUri + "?" + bodyToParams(body)
            }
        }

        var userAgent = httpReq.getHeader("User-Agent")
        if (userAgent == null || userAgent == "") userAgent = "-"
        if (!logUserAgent) userAgent = "-"

        val jsonpCallback = body.s(jsonpName, "")

        val range = httpReq.getHeader(HttpHeaders.Names.RANGE) // 断点续传

        val httpxhead = HashMapStringAny(
            "clientIpPort" -> clientIpPort,
            "remoteIpPort" -> remoteIpPort,
            "serverIpPort" -> serverIpPort,
            "userAgent" -> userAgent,
            "Range" -> range,
            "host" -> host,
            "method" -> httpReq.getMethod().toString,
            "keepAlive" -> HttpHeaders.isKeepAlive(httpReq).toString,
            "sessionId" -> body.s(sessionFieldName),
            "sessionIdChanged" -> sessionIdChanged.toString,
            "sigstat" -> (if (verify) "1" else "0"),
            "httpCode" -> "200",
            jsonpName -> jsonpCallback)

        val httpSosReq = new HttpSosRequest(requestId, connId,
            serviceId, msgId,
            httpxhead, params)

        val data = new HttpServerCacheData(httpSosReq, t)
        dataMap.put(requestId, data)

        val ret = router.send(req)
        if (ret == null) return

        dataMap.remove(requestId)
        t.cancel()
        reply(httpSosReq, ret.code, ret.res)
    }

    def replyError(httpReq: HttpRequest, connId: String, requestId: String, errorCode: Int, uri: String = "", serviceId: Int = 0, msgId: Int = 0) {

        var f = if (jsonStyle == "codeMessageNoData") errorFormatCodeMessageNoData else errorFormat
        var content = f.format(errorCode, convertErrorMessage(errorCode))

        val clientIpPort = parseClientIp(httpReq, connId)
        val remoteIpPort = parseRemoteIp(connId)
        val serverIpPort = localIp + ":" + port
        var host = httpReq.getHeader("Host")
        if (host == null || host == "") host = "unknown_host"
        var userAgent = httpReq.getHeader("User-Agent")
        if (userAgent == null || userAgent == "") userAgent = "-"

        var params = uri
        if (uri == "") params = httpReq.getUri()
        var jsonpCallback = ""
        if (params.indexOf("?") >= 0) {
            val s = params.substring(params.indexOf("?") + 1)
            jsonpCallback = parseFormField("UTF-8", s, jsonpName)
        }

        var contentType = MIMETYPE_JSON
        if (jsonpCallback != "" && contentType == MIMETYPE_JSON) {
            contentType = MIMETYPE_JAVASCRIPT
            content = """%s(%s);""".format(jsonpCallback, content)
        }

        val httpxhead = HashMapStringAny(
            "clientIpPort" -> clientIpPort,
            "remoteIpPort" -> remoteIpPort,
            "serverIpPort" -> serverIpPort,
            "userAgent" -> userAgent,
            "host" -> host,
            "method" -> httpReq.getMethod().toString,
            "keepAlive" -> HttpHeaders.isKeepAlive(httpReq).toString,
            "charset" -> "UTF-8",
            "contentType" -> contentType,
            "httpCode" -> "200")

        val req = new HttpSosRequest(requestId, connId,
            serviceId, msgId,
            httpxhead, params)

        val res = new HttpSosResponse(requestId, errorCode, content)
        val reqResInfo = new HttpSosRequestResponseInfo(req, res)

        write(connId, content, httpxhead, null, null)
        asynclog(reqResInfo)
    }

    def reply(req: HttpSosRequest, errorCode: Int, a_body: HashMapStringAny) {

        var body = a_body
        val serviceId = req.serviceId
        val msgId = req.msgId
        val connId = req.connId
        val keepAlive = req.xhead.s("keepAlive", "true")
        val params = req.params
        val charset = msgAttrs.getOrElse(serviceId + "-" + msgId + "-charset", "UTF-8")
        var contentType = msgAttrs.getOrElse(serviceId + "-" + msgId + "-responseContentType", MIMETYPE_JSON)
        var accessControlAllowOrigin = msgAttrs.getOrElse(serviceId + "-" + msgId + "-accessControlAllowOrigin", "")
        if (accessControlAllowOrigin == "" && accessControlAllowOriginGlobal != "") {
            accessControlAllowOrigin = accessControlAllowOriginGlobal
        }
        val pluginParam = msgAttrs.getOrElse(serviceId + "-" + msgId + "-pluginParam", null)
        val bodyOnly = msgAttrs.getOrElse(serviceId + "-" + msgId + "-bodyOnly", "0")
        val jsonpCallback = req.xhead.s(jsonpName, "")

        var errorMessage = convertErrorMessage(errorCode)
        if (errorCode != 0) {
            val s = fetchMessage(body)
            if (s != null && s != "") errorMessage = s
        }

        var content = ""
        val pluginObj = msgPlugins.getOrElse(serviceId + "-" + msgId, null)

        if (pluginObj != null && pluginObj.isInstanceOf[HttpServerPreOutputPlugin]) {
            body = pluginObj.asInstanceOf[HttpServerPreOutputPlugin].adjustBody(serviceId, msgId, errorCode, body)
        }

        val cookies = new HashMap[String, Cookie]()

        if (sessionMode == 1) {
            val sessionIdSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-sessionId-req", "0")
            if (sessionIdSupport == "1" && req.xhead.s("sessionIdChanged", "true") == "true") {
                val c = new DefaultCookie(sessionCookieName, req.xhead.s("sessionId", ""))
                c.setHttpOnly(sessionHttpOnly)
                if (sessionPath != "") c.setPath(sessionPath)
                if (sessionMaxAge != -1) c.setMaxAge(sessionMaxAge)
                cookies.put(sessionCookieName, c)
            }
        }
        if (sessionMode == 2) {
            val sessionIdSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-sessionId-res", "0")
            if (sessionIdSupport == "1") {
                val sessionId = body.s(sessionFieldName, "")
                if (sessionId != "") {
                    val c = new DefaultCookie(sessionCookieName, sessionId)
                    c.setHttpOnly(sessionHttpOnly)
                    if (sessionPath != "") c.setPath(sessionPath)
                    if (sessionMaxAge != -1) c.setMaxAge(sessionMaxAge)
                    cookies.put(sessionCookieName, c)
                }
                body.remove(sessionFieldName)
            }
        }

        val cookieBuff = cookieMap.getOrElse(serviceId + "-" + msgId + "-res", null)
        if (cookieBuff != null && cookieBuff.size > 0) {
            for ((fieldName, cookieName, opt) <- cookieBuff) {
                val c = new DefaultCookie(cookieName, body.s(fieldName, ""))
                if (opt != null && opt != "")
                    updateCookieOption(c, opt)
                cookies.put(c.getName, c)
                body.remove(fieldName)
            }
        }

        val headers = new HashMap[String, String]()

        val headerBuff = headerMap.getOrElse(serviceId + "-" + msgId + "-res", null)
        if (headerBuff != null && headerBuff.size > 0) {
            for ((fieldName, headerName) <- headerBuff) {
                val v = body.s(fieldName, "")
                if (v != null && v != "") headers.put(headerName, v)
                body.remove(fieldName)
            }
        }

        if (!headers.contains("Access-Control-Allow-Origin") && accessControlAllowOrigin != "") {
            headers.put("Access-Control-Allow-Origin", accessControlAllowOrigin)
        }

        val redirectUrl302 = body.s(redirect302FieldName, "")
        if (redirectUrl302 != "") {

            write302(connId, req.xhead, redirectUrl302, cookies)

            val reqResInfo = new HttpSosRequestResponseInfo(req, new HttpSosResponse(req.requestId, 0, ""))
            req.xhead.put("contentType", MIMETYPE_PLAIN)
            req.xhead.put("charset", "UTF-8")
            req.xhead.put("httpCode", "302")
            asynclog(reqResInfo)
            return
        }

        var rawContent: Array[Byte] = null
        var staticFile: String = null

        if (pluginObj != null && pluginObj.isInstanceOf[HttpServerRawOutputPlugin]) {

            if (errorCode == 0) {
                //val domainName = getDomainName(req.xhead.s("host",""))
                //if( !body.contains(domainNameFieldName) ) body.put(domainNameFieldName,domainName)
                //val contextPath = getContextPath(params)
                //if( contextPath != "" && !body.contains("contextPath") ) body.put("contextPath",contextPath)
                //if( urlArgs != "" && !body.contains("urlArgs") ) body.put("urlArgs",urlArgs)
                rawContent = pluginObj.asInstanceOf[HttpServerRawOutputPlugin].generateRawContent(serviceId, msgId, errorCode, errorMessage, body, pluginParam, headers)
                content = "raw_content:" + (if (rawContent == null) 0 else rawContent.length)

                val ext = body.remove("__file_ext__")
                if (!ext.isEmpty) {
                    contentType = mimeTypeMap.getOrElse(ext.get.toString, contentType)
                }
            }
            if (rawContent == null) {
                write404InReply(req, errorCode)
                return
            }

        } else if (pluginObj != null && pluginObj.isInstanceOf[HttpServerStaticFilePlugin]) {

            if (errorCode == 0) {
                //val domainName = getDomainName(req.xhead.s("host",""))
                //if( !body.contains(domainNameFieldName) ) body.put(domainNameFieldName,domainName)
                //val contextPath = getContextPath(params)
                //if( contextPath != "" && !body.contains("contextPath") ) body.put("contextPath",contextPath)
                //if( urlArgs != "" && !body.contains("urlArgs") ) body.put("urlArgs",urlArgs)
                staticFile = pluginObj.asInstanceOf[HttpServerStaticFilePlugin].generateStaticFile(serviceId, msgId, errorCode, errorMessage, body, pluginParam, headers)
                content = "static_file:" + (if (staticFile == null) "not found" else staticFile + ":" + new File(staticFile).length())

                val ext = body.remove("__file_ext__")
                if (!ext.isEmpty) {
                    contentType = mimeTypeMap.getOrElse(ext.get.toString, contentType)
                }
            }
            if (staticFile == null) {
                write404InReply(req, errorCode)
                return
            }

        } else if (pluginObj != null && pluginObj.isInstanceOf[HttpServerOutputPlugin]) {

            val domainName = getDomainName(req.xhead.s("host", ""))
            if (!body.contains(domainNameFieldName)) body.put(domainNameFieldName, domainName)
            val contextPath = getContextPath(params)
            if (contextPath != "" && !body.contains("contextPath")) body.put("contextPath", contextPath)
            if (urlArgs != "" && !body.contains("urlArgs")) body.put("urlArgs", urlArgs)
            content = pluginObj.asInstanceOf[HttpServerOutputPlugin].generateContent(serviceId, msgId, errorCode, errorMessage, body, pluginParam)
            val ext = body.remove("__file_ext__")
            if (!ext.isEmpty) {
                contentType = mimeTypeMap.getOrElse(ext.get.toString, contentType)
            }

        } else {

            if (TypeSafe.isTrue(bodyOnly)) {

                content = JsonCodec.mkString(jsonTypeProcess(req.serviceId, req.msgId, body))

            } else {
                val map = HashMapStringAny()
                if (jsonStyle == "codeMessageNoData") {
                    map ++= jsonTypeProcess(req.serviceId, req.msgId, body)
                    map.put("code", errorCode)
                    if (!map.contains("message"))
                        map.put("message", errorMessage)
                } else {
                    map.put("return_code", errorCode)
                    map.put("return_message", errorMessage)
                    map.put("data", jsonTypeProcess(req.serviceId, req.msgId, body))
                }
                content = JsonCodec.mkString(map)
            }
        }

        if (jsonpCallback != "" && contentType == MIMETYPE_JSON) {
            contentType = MIMETYPE_JAVASCRIPT
            content = """%s(%s);""".format(jsonpCallback, content)
        }
        val reqResInfo = new HttpSosRequestResponseInfo(req, new HttpSosResponse(req.requestId, errorCode, content))

        req.xhead.put("contentType", contentType)
        req.xhead.put("charset", charset)

        if (staticFile != null) {
            writeStaticFileInReply(connId, staticFile, req.xhead, cookies, headers, reqResInfo)
            return
        }

        if (rawContent != null)
            writeRaw(connId, rawContent, req.xhead, cookies, headers)
        else
            write(connId, content, req.xhead, cookies, headers)
        asynclog(reqResInfo)
    }
    
    def write302(connId: String, xhead: HashMapStringAny, url: String, cookies: HashMap[String, Cookie]) {
        val keepAlive = xhead.s("keepAlive", "true") == "true"
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FOUND)
        setDateHeader(response)
        if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")
        response.setHeader(HttpHeaders.Names.LOCATION, url)
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, "0")

        if (cookies != null && cookies.size > 0) {
            for ((dummy, c) <- cookies) {
                val encoder = new CookieEncoder(true)
                encoder.addCookie(c)
                response.addHeader(HttpHeaders.Names.SET_COOKIE, encoder.encode())
            }
        }

        nettyHttpServer.write(connId, response, keepAlive)
    }

    def write(connId: String, content: String, xhead: HashMapStringAny, cookies: HashMap[String, Cookie], headers: HashMap[String, String]) {
        val charset = xhead.s("charset", "UTF-8")
        val rawContent = content.getBytes(charset)
        writeRaw(connId, rawContent, xhead, cookies, headers)
    }

    def writeRaw(connId: String, rawContent: Array[Byte], xhead: HashMapStringAny, cookies: HashMap[String, Cookie], headers: HashMap[String, String]) {

        val method = xhead.s("method", "POST")
        val keepAlive = xhead.s("keepAlive", "true") == "true"
        val contentType = xhead.s("contentType", "")

        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

        response.setHeader(HttpHeaders.Names.SERVER, "scalabpe httpserver/1.1.0")
        setDateHeader(response, true)

        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)
        val buff = ChannelBuffers.wrappedBuffer(rawContent)
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buff.readableBytes()))

        if (cookies != null && cookies.size > 0) {
            for ((dummy, c) <- cookies) {
                val encoder = new CookieEncoder(true)
                encoder.addCookie(c)
                response.addHeader(HttpHeaders.Names.SET_COOKIE, encoder.encode())
            }
        }

        if (headers != null) {
            for ((key, value) <- headers) {
                response.setHeader(key, value)
            }
        }

        if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")

        if (method == "HEAD") {
            nettyHttpServer.write(connId, response, keepAlive)
            return
        }

        //if( log.isDebugEnabled)
        //    log.debug("http reply: " + content)

        response.setContent(buff)
        nettyHttpServer.write(connId, response, keepAlive)
    }

    def write404InReply(req: HttpSosRequest, errorCode: Int) {
        val keepAlive = req.xhead.s("keepAlive", "true") == "true"
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
        setDateHeader(response)
        if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")
        val s = "FILE_NOT_FOUND"
        val buff = ChannelBuffers.wrappedBuffer(s.getBytes())
        response.setContent(buff)
        val contentType = "text/plain"
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buff.readableBytes()))
        nettyHttpServer.write(req.connId, response, keepAlive)

        val reqResInfo = new HttpSosRequestResponseInfo(req, new HttpSosResponse(req.requestId, errorCode, s))
        req.xhead.put("contentType", MIMETYPE_PLAIN)
        req.xhead.put("charset", "UTF-8")
        req.xhead.put("httpCode", "404")
        asynclog(reqResInfo)
    }

    def write404(httpReq: HttpRequest, connId: String, xhead: HashMapStringAny, requestId: String, uri: String) {
        val keepAlive = HttpHeaders.isKeepAlive(httpReq)
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
        setDateHeader(response)
        if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")
        val buff = ChannelBuffers.wrappedBuffer("FILE_NOT_FOUND".getBytes())
        response.setContent(buff)
        val contentType = "text/plain"
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buff.readableBytes()))
        nettyHttpServer.write(connId, response, keepAlive)
        val reqResInfo = logStaticFile(requestId, httpReq, connId, HttpResponseStatus.NOT_FOUND, contentType, buff.readableBytes())
        router.asyncLogActor.receive(reqResInfo)
    }

    def write304(httpReq: HttpRequest, connId: String, xhead: HashMapStringAny, requestId: String, uri: String, f: File): Boolean = {

        val keepAlive = HttpHeaders.isKeepAlive(httpReq)

        val ifModifiedSince = httpReq.getHeader(HttpHeaders.Names.IF_MODIFIED_SINCE)

        if (ifModifiedSince != null && ifModifiedSince != "") {
            val ifModifiedSinceDate = df_tl.get.parse(ifModifiedSince)
            val ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000
            val fileLastModifiedSeconds = f.lastModified() / 1000
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_MODIFIED)
                setDateHeader(response)
                if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")
                nettyHttpServer.write(connId, response, keepAlive)
                val ext = getExt(f.getName)
                val contentType = mimeTypeMap.getOrElse(ext, MIMETYPE_DEFAULT)
                val reqResInfo = logStaticFile(requestId, httpReq, connId, HttpResponseStatus.NOT_MODIFIED, contentType, f.length())
                router.asyncLogActor.receive(reqResInfo)
                return true
            }
        }

        false
    }

    def writeStaticFileInReply(connId: String, staticFile: String, xhead: HashMapStringAny, cookies: HashMap[String, Cookie], headers: HashMap[String, String], reqResInfo: HttpSosRequestResponseInfo) {

        val method = xhead.s("method", "POST")
        val keepAlive = xhead.s("keepAlive", "true") == "true"
        val contentType = xhead.s("contentType", "")

        val f = new File(staticFile)
        val fileLength = f.length()
        var range = xhead.ns("Range") // 断点续传
        var range_tpl:Tuple2[Long,Long] = (-1,-1)
        if( range != null && range != "" ) {
            range_tpl = parseRange(range,fileLength)
        }
        val response = 
            if( range_tpl._1 != -1 ) 
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT)
            else
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

        response.setHeader(HttpHeaders.Names.SERVER, "scalabpe httpserver/1.1.0")

        val time = new GregorianCalendar();
        response.setHeader(HttpHeaders.Names.DATE, df_tl.get().format(time.getTime()));
        response.setHeader(HttpHeaders.Names.LAST_MODIFIED, df_tl.get.format(new Date(f.lastModified())));

        if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")

        if (cookies != null && cookies.size > 0) {
            for ((dummy, c) <- cookies) {
                val encoder = new CookieEncoder(true)
                encoder.addCookie(c)
                response.addHeader(HttpHeaders.Names.SET_COOKIE, encoder.encode())
            }
        }

        if (headers != null) {
            for ((key, value) <- headers) {
                response.setHeader(key, value)
            }
        }

        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)

        if( range_tpl._1 != -1 ) {
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(range_tpl._2 - range_tpl._1 + 1));
            val s = "bytes %d-%d/%d".format(range_tpl._1,range_tpl._2,fileLength)
            response.setHeader(HttpHeaders.Names.CONTENT_RANGE, s);
        } else {
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(fileLength));
        }

        if (method == "HEAD") {
            nettyHttpServer.write(connId, response, keepAlive)
            router.asyncLogActor.receive(reqResInfo)
            return
        }

        nettyHttpServer.writeFileZeroCopy(connId, response, keepAlive, f, fileLength, range_tpl, reqResInfo)
    }

    def parseRange(s:String,fileLength:Long):Tuple2[Long,Long] = {
        try {
            val ss = s.trim.split("=")
            if( ss.length != 2) return (-1,-1)
            if( ss(0) != "bytes") return (-1,-1)
            val p = ss(1).indexOf("-")
            if( p == -1 ) return (-1,-1)
            var min = ss(1).substring(0,p)
            var max = ss(1).substring(p+1)
            if( min == "" && max == "" ) return (-1,-1)
            if( min == "" && max != "" ) { 
                min = (fileLength - max.toLong - 1).toString
                max = (fileLength - 1).toString
            } else {
                if( max == "" ) max = (fileLength - 1).toString
            }
            val min_l = min.toLong 
            val max_l = max.toLong 
            if( min_l > max_l ) return (-1,-1)
            (min_l,max_l)
        } catch {
            case e:Throwable =>
                return (-1,-1)
        }
    }

    def writeStaticFile(httpReq: HttpRequest, connId: String, xhead: HashMapStringAny, requestId: String, uri: String, f: File) {

        val keepAlive = HttpHeaders.isKeepAlive(httpReq)

        if (write304(httpReq, connId, xhead, requestId, uri, f)) return

        /*
            Range头域可以请求实体的一个或者多个子范围。例如，
            表示头500个字节：bytes=0-499
            表示第二个500字节：bytes=500-999
            表示500字节以后的范围：bytes=500-
            表示最后500个字节：bytes=-500
            第一个和最后一个字节：bytes=0-0,-1  不支持
            同时指定几个范围：bytes=500-600,601-999 不支持
        */
        val fileLength = f.length()
        var range = httpReq.getHeader(HttpHeaders.Names.RANGE) // 断点续传
        var range_tpl:Tuple2[Long,Long] = (-1,-1)
        if( range != null && range != "" ) {
            range_tpl = parseRange(range,fileLength)
        }
        val response = 
            if( range_tpl._1 != -1 ) 
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT)
            else
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

        response.setHeader(HttpHeaders.Names.SERVER, "scalabpe httpserver/1.1.0")
        setDateHeaderAndCache(response, f)

        if (!keepAlive) response.setHeader(HttpHeaders.Names.CONNECTION, "close")
        val ext = getExt(f.getName)

        val contentType = mimeTypeMap.getOrElse(ext, MIMETYPE_DEFAULT)
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)

        if( range_tpl._1 != -1 ) {
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(range_tpl._2 - range_tpl._1 + 1));
            val s = "bytes %d-%d/%d".format(range_tpl._1,range_tpl._2,fileLength)
            response.setHeader(HttpHeaders.Names.CONTENT_RANGE, s);
        } else {
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(fileLength));
        }
        /*

    If-Modified-Since   如果请求的部分在指定时间之后被修改则请求成功，未被修改则返回304代码 If-Modified-Since: Sat, 29 Oct 2010 19:43:31 GMT
    If-Match    只有请求内容与实体相匹配才有效  If-Match: “737060cd8c284d8af7ad3082f209582d”
    If-None-Match   如果内容未改变返回304代码，参数为服务器先前发送的Etag，与服务器回应的Etag比较判断是否改变   If-None-Match: “737060cd8c284d8af7ad3082f209582d”
    If-Range    如果实体未改变，服务器发送客户端丢失的部分，否则发送整个实体。参数也为Etag  If-Range: “737060cd8c284d8af7ad3082f209582d”
    If-Unmodified-Since 只在实体在指定时间之后未被修改才请求成功    If-Unmodified-Since: Sat, 29 Oct 2010 19:43:31 GMT

    Date: Wed, 09 Dec 2015 01:47:15 GMT
    Last-Modified: Fri, 27 Apr 2012 09:05:32 GMT
    Expires: Wed, 09 Dec 2015 01:49:15 GMT
    Cache-Control: max-age=120

    Date	原始服务器消息发出的时间
    Last-Modified ：指出原始服务器认为该变量最后修改的日期和时间，确实意思取决于原是服务器的实现和资源的属性。对文件，可能只是文件系统内最后修改时间
    Expires ：指出响应被认为过期的日期/时间
    Cache-Control   告诉所有的缓存机制是否可以缓存及哪种类型    Cache-Control: no-cache

    Accept-Ranges: bytes
    AGE: 38
    ETAGS: "xxx"

     */

        val reqResInfo = logStaticFile(requestId, httpReq, connId, HttpResponseStatus.OK, contentType, fileLength)

        if (httpReq.getMethod() == HttpMethod.HEAD) {
            nettyHttpServer.write(connId, response, keepAlive)
            router.asyncLogActor.receive(reqResInfo)
            return
        }

        if (cacheEnabled && fileLength <= cacheFileSize && cacheFilesSet.contains(ext) && range_tpl._1 != -1) {
            var data = cacheMap.get(f.getPath)
            if (data == null) {
                data = FileUtils.readFileToByteArray(f)
                cacheMap.put(f.getPath, data)
            }

            val buff = ChannelBuffers.wrappedBuffer(data)
            response.setContent(buff)
            nettyHttpServer.write(connId, response, keepAlive, reqResInfo)
            return
        }

        nettyHttpServer.writeFile(connId, response, keepAlive, f, fileLength, range_tpl, reqResInfo)
    }

    def parseBody(requestId: String, serviceId: Int, msgId: Int, httpReq: HttpRequest, host: String, clientIp: String): Tuple2[HashMapStringAny, Boolean] = {

        var contentType = msgAttrs.getOrElse(serviceId + "-" + msgId + "-requestContentType", "")
        if (contentType == "") {
            var s = httpReq.getHeader(HttpHeaders.Names.CONTENT_TYPE)
            if (s == null) s = ""
            if (s.indexOf(MIMETYPE_FORM) >= 0) contentType = MIMETYPE_FORM
            else if (s.indexOf(MIMETYPE_JSON) >= 0) contentType = MIMETYPE_JSON
            else if (s.indexOf(MIMETYPE_XML) >= 0 || s.indexOf(MIMETYPE_XML2) >= 0) contentType = MIMETYPE_XML
            else if (s.indexOf(MIMETYPE_MULTIPART) >= 0) contentType = MIMETYPE_MULTIPART
        }
        if (contentType == "") {
            contentType = MIMETYPE_FORM
        }

        val charset = msgAttrs.getOrElse(serviceId + "-" + msgId + "-charset", "UTF-8")
        val method = httpReq.getMethod()

        val map = HashMapStringAny()
        var sessionIdChanged = false

        val uri = httpReq.getUri()
        if (uri.indexOf("?") >= 0) {
            val s = uri.substring(uri.indexOf("?") + 1)
            parseFormContent(charset, s, map)
        }

        val pluginObj = msgPlugins.getOrElse(serviceId + "-" + msgId, null)
        if (method == HttpMethod.POST && contentType == MIMETYPE_MULTIPART) {

            if (log.isDebugEnabled) {
                log.debug("http file upload, headers: " + toHeaders(httpReq))
            }

            val x_upload_processed = httpReq.getHeader("X_UPLOAD_PROCESSED")
            val x_upload_processed_2 = httpReq.getHeader("X_UPLOAD_PROCESSED_2")
            if (x_upload_processed != null && x_upload_processed != "") {
                val t1 = System.currentTimeMillis()
                val in = new BufferedInputStream(new FileInputStream(x_upload_processed), 5000000)
                parseFileUploadContent(charset, in, map)
                in.close()
                new File(x_upload_processed).delete()
                val t2 = System.currentTimeMillis()
                if( t2 - t1 > 1000 ) {
                    log.warn("parse file upload content ts="+(t2-t1)+", file="+x_upload_processed)
                }
            } else if (x_upload_processed_2 != null && x_upload_processed_2 != "") {
                parseFileUploadContent2(charset, x_upload_processed_2, map)
            } else {
                // upload file 
                // upload file to temp dir and put filename to body
                val content = httpReq.getContent()
                val in = new ChannelBufferInputStream(content)
                parseFileUploadContent(charset, in, map)
            }

        } else if (method == HttpMethod.POST) {
            val content = httpReq.getContent()
            val contentStr = content.toString(Charset.forName(charset))
            if (log.isDebugEnabled) {
                log.debug("http post content: " + contentStr + ", headers: " + toHeaders(httpReq))
            }
            if (contentType == MIMETYPE_FORM) {
                parseFormContent(charset, contentStr, map)
            } else {
                if (pluginObj != null && pluginObj.isInstanceOf[HttpServerRequestParsePlugin])
                    pluginObj.asInstanceOf[HttpServerRequestParsePlugin].parseContent(serviceId, msgId, charset, contentType, contentStr, map)
            }
        }

        val headerBuff = headerMap.getOrElse(serviceId + "-" + msgId + "-req", null)
        if (headerBuff != null && headerBuff.size > 0) {
            for ((fieldName, headerName) <- headerBuff) {
                val v = httpReq.getHeader(headerName)
                if (v != null) map.put(fieldName, v)
            }
        }

        map.remove(sessionFieldName) // 总是从cookie中取值

        val sessionIdSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-sessionId-req", "0")
        if (sessionIdSupport == "1") {
            var sessionId = ""
            val cookie = httpReq.getHeader("Cookie")
            if (cookie != null && cookie != "") {
                val cookies = new CookieDecoder().decode(cookie)
                if (cookies != null) {
                    val it = cookies.iterator()
                    while (it.hasNext()) {
                        val c = it.next()
                        if (c.getName == sessionCookieName) {
                            sessionId = c.getValue
                        }
                    }
                }
            }
            if (sessionId != "" && sessionMode == 1) {
                if (!validateSessionId(sessionId, host, clientIp)) {
                    sessionId = ""
                }
            }
            if (sessionId == "" && sessionMode == 1) {
                sessionId = genSessionId(requestId, host, clientIp)
                sessionIdChanged = true
            }

            if (sessionId != "")
                map.put(sessionFieldName, sessionId)
        }

        val cookieBuff = cookieMap.getOrElse(serviceId + "-" + msgId + "-req", null)
        if (cookieBuff != null && cookieBuff.size > 0) {
            val cookie = httpReq.getHeader("Cookie")
            val m = HashMapStringString()
            if (cookie != null && cookie != "") {
                val cookies = new CookieDecoder().decode(cookie)
                if (cookies != null) {
                    val it = cookies.iterator()
                    while (it.hasNext()) {
                        val c = it.next()
                        m.put(c.getName, c.getValue)
                    }
                }
            }
            for ((fieldName, cookieName, dummy) <- cookieBuff) {
                map.put(fieldName, m.getOrElse(cookieName, ""))
            }
        }

        val requestUriSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-" + requestUriFieldName, "0")
        if (requestUriSupport == "1") {
            val uri = httpReq.getUri()
            if (uri.indexOf("?") >= 0) {
                val s = uri.substring(0,uri.indexOf("?"))
                map.put(requestUriFieldName, s)
            } else {
                map.put(requestUriFieldName, uri)
            }
        }

        val domainNameSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-" + domainNameFieldName, "1")
        if (domainNameSupport == "1") {
            val s = httpReq.getHeader("host")
            val domainName = getDomainName(s)
            map.put(domainNameFieldName, domainName)
        }

        val queryStringSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-" + queryStringFieldName, "0")
        if (queryStringSupport == "1") {
            val uri = httpReq.getUri()
            if (uri.indexOf("?") >= 0) {
                val s = uri.substring(uri.indexOf("?") + 1)
                map.put(queryStringFieldName, s)
            } else {
                map.put(queryStringFieldName, "")
            }
        }

        val contentTypeSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-" + contentTypeFieldName, "0")
        if (contentTypeSupport == "1" && method == HttpMethod.POST) {
            map.put(contentTypeFieldName, contentType)
        }

        val contentDataSupport = msgAttrs.getOrElse(serviceId + "-" + msgId + "-" + contentDataFieldName, "0")
        if (contentDataSupport == "1" && method == HttpMethod.POST) {
            val content = httpReq.getContent()
            val contentStr = content.toString(Charset.forName(charset))
            map.put(contentDataFieldName, contentStr)
        }

        (map, sessionIdChanged)
    }

    def asynclog(reqResInfo: HttpSosRequestResponseInfo) {

        val buff = logFilterMap.getOrElse(reqResInfo.req.serviceId + "-" + reqResInfo.req.msgId, null)
        if (buff != null && buff.size > 0) {
            for (r <- buff) {
                reqResInfo.req.params = r.replaceAllIn(reqResInfo.req.params, "")
                reqResInfo.res.content = r.replaceAllIn(reqResInfo.res.content, "")
            }
        }

        router.asyncLogActor.receive(reqResInfo)
    }

    def genSessionId(requestId: String, host: String, clientIp: String): String = {
        val t = requestId
        if (!sessionIpBind) return t
        val data = t + "#" + clientIp
        val s = CryptHelper.encryptHex(CryptHelper.ALGORITHM__DES, sessionEncKey, data)
        s
    }
    
    def validateSessionId(sessionId: String, host: String, clientIp: String): Boolean = {
        if (!sessionIpBind) return true
        try {
            val s = CryptHelper.decryptHex(CryptHelper.ALGORITHM__DES, sessionEncKey, sessionId)
            if (s == null) return false
            val ss = s.split("#")
            if (ss.length != 2) return false
            if (ss(1) != clientIp) return false
            true
        } catch {
            case _: Throwable => false
        }
    }

    def getMerchantInfo(body: HashMapStringAny): Tuple2[Boolean, String] = {

        val merchant_name = body.s("merchant_name", "")
        val signature_method = body.s("signature_method", "")
        val signature = body.s("signature", "")
        val timestamp = body.s("timestamp", "")

        if (merchant_name == "") return new Tuple2(false, null)
        if (signature_method == "") return new Tuple2(false, null)
        if (signature == "") return new Tuple2(false, null)
        if (timestamp == "") return new Tuple2(false, null)

        (true, merchant_name)
    }

    def standardVerify(serviceId: Int, msgId: Int, xhead: HashMapStringAny, body: HashMapStringAny, cfg: OsapMerchantCfg, uri: String): Boolean = {

        val signMap = new java.util.TreeMap[String, String]()

        for ((key, value) <- body if key != "signature") { // donot ignore sessionId,cookie,header field, in that case standardVerify will not be called
            signMap.put(key, value.toString)
        }

        val signString = new StringBuilder()

        val entries = signMap.entrySet().iterator()
        while (entries.hasNext()) {
            val entry = entries.next()
            signString.append(entry.getKey).append("=").append(entry.getValue)
        }

        val reqSignature = body.s("signature")
        var ok = false
        val now = System.currentTimeMillis
        for (key <- cfg.md5Key if key.st <= now && key.et >= now) {
            val s = signString.append(key.key)
            val signature = CryptHelper.md5(signString.toString)
            if (signature == reqSignature) ok = true
        }

        if (!ok) {
            log.error("verify failed, md5 verify error, uri=" + uri)
            return false
        }

        val clientIpPort = xhead.ns(Xhead.KEY_FIRST_ADDR)
        val clientIp = clientIpPort.substring(0, clientIpPort.indexOf(":"))

        if (!cfg.ipSet.contains(clientIp)) {
            log.error("verify failed, ip verify error, uri=" + uri)
            return false
        }

        if (!cfg.privilegeSet.contains(serviceId + "-" + msgId)) {
            log.error("verify failed, privilege verify error, uri=" + uri)
            return false
        }

        xhead.put(Xhead.KEY_SOC_ID, cfg.merchantName)
        xhead.put(Xhead.KEY_APP_ID, cfg.appId)
        xhead.put(Xhead.KEY_AREA_ID, cfg.areaId)

        true
    }

    def bodyToParams(body: HashMapStringAny): String = {
        if (body == null) return ""
        val buff = new StringBuilder
        for ((key, value) <- body) {
            if (buff.length > 0) buff.append("&")
            buff.append(key + "=" + URLEncoder.encode(value.toString, "UTF-8"))
        }
        buff.toString
    }

    def mappingUrl(uri: String): Tuple4[Int, Int, String, ArrayBufferPattern] = {

        var part = uri.toLowerCase
        val p = part.indexOf("?")
        if (p >= 0) {
            val params = part.substring(p + 1)
            part = part.substring(0, p)
            val p1 = params.indexOf("method=")
            if (p1 >= 0) {
                var method = parseFormField("UTF-8", params, "method")
                if (method != "") {
                    part += "/" + method
                }
            }
        }

        if (part.startsWith("/")) {
            part = part.substring(1)
        }

        mappingUrlInternal(part)
    }

    def mappingUrlInternal(uri: String): Tuple4[Int, Int, String, ArrayBufferPattern] = {

        var part = uri
        var hasSlash = false
        var tpl: Tuple4[Int, Int, String, ArrayBufferPattern] = null
        do {
            tpl = urlMapping.getOrElse(part, null)
            if (tpl != null) return tpl

            if (hasPattern) {
                tpl = urlMapping.getOrElse(part + "/*/*", null)
                if (tpl != null) return tpl
                tpl = urlMapping.getOrElse(part + "/*", null)
                if (tpl != null) return tpl
            }

            val t = part.lastIndexOf("/")
            hasSlash = t >= 0

            if (hasSlash) {
                part = part.substring(0, t)
            }

        } while (hasSlash)

        if (hasPattern) {
            tpl = urlMapping.getOrElse("*/*", null)
            if (tpl != null) return tpl
            tpl = urlMapping.getOrElse("*", null)
            if (tpl != null) return tpl
        }

        (0, 0, "", null)
    }

    def mappingStaticUrl(uri: String): String = {

        if (!webappStaticDirExisted) return ""
        if (uri.indexOf("..") >= 0) return ""

        var part = uri
        val p = uri.indexOf("?")
        if (p >= 0) {
            part = uri.substring(0, p)
        }
        if (part.startsWith("/")) {
            part = part.substring(1)
        }

        if (skipMinFile) {
            part = StringUtils.replace(part, ".min.", ".")
        }

        if (part == "") part = "index.html"
        if (part.endsWith("/")) part += "index.html"

        val f = webappStaticDir + File.separator + part
        val file = new File(f)
        if (!file.exists() || file.isHidden() || file.isDirectory()) return ""
        if (!file.getPath.startsWith(webappStaticDir)) return ""
        f
    }

    def logStaticFile(requestId: String, httpReq: HttpRequest, connId: String, httpCode: HttpResponseStatus, contentType: String, contentLength: Long): HttpSosRequestResponseInfo = {

        val clientIpPort = parseClientIp(httpReq, connId)
        val remoteIpPort = parseRemoteIp(connId)
        val serverIpPort = localIp + ":" + port
        var host = httpReq.getHeader("Host")
        if (host == null || host == "") host = "unknown"
        val keepAlive = HttpHeaders.isKeepAlive(httpReq).toString
        var userAgent = httpReq.getHeader("User-Agent")
        if (userAgent == null || userAgent == "") userAgent = "-"

        var params = httpReq.getUri()

        val httpxhead = HashMapStringAny(
            "clientIpPort" -> clientIpPort,
            "remoteIpPort" -> remoteIpPort,
            "userAgent" -> userAgent,
            "serverIpPort" -> serverIpPort,
            "host" -> host,
            "method" -> httpReq.getMethod().toString,
            "keepAlive" -> keepAlive,
            "contentType" -> contentType,
            "contentLength" -> contentLength,
            "staticFile" -> "1",
            "httpCode" -> httpCode.getCode.toString)

        val req = new HttpSosRequest(requestId, connId,
            0, 0,
            httpxhead, params)

        val res = new HttpSosResponse(requestId, httpCode.getCode, "")
        val reqResInfo = new HttpSosRequestResponseInfo(req, res)
        reqResInfo
    }

    def setDateHeader(response: HttpResponse, addNoCache: Boolean = false) {
        val time = new GregorianCalendar();
        response.setHeader(HttpHeaders.Names.DATE, df_tl.get().format(time.getTime()));
        if (addNoCache)
            response.setHeader(HttpHeaders.Names.CACHE_CONTROL, "no-cache");
    }

    def setDateHeaderAndCache(response: HttpResponse, f: File) {
        val time = new GregorianCalendar();
        response.setHeader(HttpHeaders.Names.DATE, df_tl.get().format(time.getTime()));
        time.add(Calendar.SECOND, httpCacheSeconds);
        response.setHeader(HttpHeaders.Names.EXPIRES, df_tl.get.format(time.getTime()));
        response.setHeader(HttpHeaders.Names.LAST_MODIFIED, df_tl.get.format(new Date(f.lastModified())));
        if (httpCacheSeconds == 0)
            response.setHeader(HttpHeaders.Names.CACHE_CONTROL, "no-cache");
        else
            response.setHeader(HttpHeaders.Names.CACHE_CONTROL, "max-age=" + httpCacheSeconds);
    }

    def getDomainName(s: String): String = {
        val p = s.indexOf(":")
        if (p >= 0) return s.substring(0, p)
        else s
    }
    def getContextPath(params: String): String = {
        val p = params.indexOf("/")
        if (p != 0) return "/"
        val p2 = params.indexOf("/", p + 1)
        if (p2 > 0) return params.substring(0, p2)
        val p3 = params.indexOf("?", p + 1)
        if (p3 > 0) return params.substring(0, p3)
        else params
    }

    def getExt(f: String): String = {
        val p = f.lastIndexOf(".")
        if (p < 0) return ""
        val ext = f.substring(p + 1).toLowerCase
        ext
    }

    def parseIp(s: String): String = {
        val p = s.lastIndexOf(":")
        if (p >= 0)
            s.substring(0, p)
        else
            s
    }

    def parseRemoteIp(connId: String): String = {
        var clientIp = parseIp(connId)
        clientIp
    }

    def parseClientIp(httpReq: HttpRequest, connId: String): String = {
        var clientIp = parseIp(connId)

        val xff = httpReq.getHeader("X-Forwarded-For")
        if (xff != null && xff != "") {
            val ss = xff.split(",")
            if (ss.length > 0) clientIp = ss(0).trim
        }

        if (clientIp.indexOf(":") < 0) clientIp += ":80"

        clientIp
    }

    def parseHttpType(httpReq: HttpRequest): String = {

        var ht = httpReq.getHeader("HTTP_X_Forwarded_Proto")
        if (ht == null || ht == "") {
            ht = httpReq.getHeader("X_HTTP_SCHEME")
        }
        if (ht == null) return null
        val s = ht match {
            case "http"  => "1"
            case "https" => "2"
            case _       => null
        }

        s
    }

    def parseXhead(httpReq: HttpRequest, connId: String, requestId: String): HashMapStringAny = {
        var clientIp = parseClientIp(httpReq, connId)
        val map = HashMapStringAny()
        map.put(Xhead.KEY_ADDRS, ArrayBufferString(clientIp))
        map.put(Xhead.KEY_FIRST_ADDR, clientIp)
        map.put(Xhead.KEY_LAST_ADDR, clientIp)
        map.put(Xhead.KEY_UNIQUE_ID, requestId)

        val httpType = parseHttpType(httpReq)
        if (httpType != null) {
            map.put(Xhead.KEY_HTTP_TYPE, httpType)
        }

        map
    }

    def updateCookieOption(c: Cookie, opt: String) {
        val ss = opt.split(";")
        for (s <- ss) {
            val ss2 = s.split("=")
            ss2(0) match {
                case "Path"     => c.setPath(ss2(1))
                case "MaxAge"   => c.setMaxAge(ss2(1).toInt)
                case "Domain"   => c.setDomain(ss2(1))
                case "Ports"    => c.setPorts(ss2(1).toInt) // support only one port
                case "HttpOnly" => c.setHttpOnly(TypeSafe.isTrue(ss2(1)))
                case "Discard"  => c.setDiscard(TypeSafe.isTrue(ss2(1)))
                case "Secure"   => c.setSecure(TypeSafe.isTrue(ss2(1)))
                case "Version"  => c.setVersion(ss2(1).toInt)
                case _          =>
            }
        }
    }

    def convertBodyCaseInsensitive(serviceId: Int, msgId: Int, body: HashMapStringAny): HashMapStringAny = {
        val tlvCodec = router.findTlvCodec(serviceId)
        if (tlvCodec == null) return null

        val newbody = HashMapStringAny()
        for ((key, value) <- body if value != null) {
            newbody.put(key.toLowerCase, value)
        }

        val map = new HashMapStringAny()
        val keyMap = tlvCodec.msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP)
        for ((key, value) <- keyMap) {
            val v = newbody.s(key.toLowerCase, null)
            if (v != null)
                map.put(key, v)
        }

        map
    }

    def uuid(): String = {
        return java.util.UUID.randomUUID().toString().replaceAll("-", "")
    }

    def parseFormContent(charSet: String, contentStr: String, body: HashMapStringAny) {

        val ss = contentStr.split("&")

        for (s <- ss) {
            val tt = s.split("=")
            if (tt.size >= 2) {
                val key = tt(0)
                val value = URLDecoder.decode(tt(1), charSet)
                var s = body.s(key)
                if (s != null && s != "") s += "," + value
                else s = value
                body.put(key, s)
            }
        }

    }

    def parseFormField(charSet: String, s: String, field: String): String = {

        val contentStr = "&" + s
        val p1 = contentStr.indexOf("&" + field + "=")
        if (p1 < 0) return ""
        val p2 = contentStr.indexOf("&", p1 + 1)
        var value = ""
        if (p2 < 0) {
            value = contentStr.substring(p1 + 1 + field.length + 1)
        } else {
            value = contentStr.substring(p1 + 1 + field.length + 1, p2)
        }
        value = URLDecoder.decode(value, charSet)
        value
    }

    def fetchMessage(body: HashMapStringAny): String = {
        for (key <- returnMessageFieldNames) {
            val s = body.s(key, null)
            if (s != null && removeReturnMessageInBody) body.remove(key)
            if (s != null && s != "") return s
        }
        null
    }

    def convertErrorMessage(errorCode: Int): String = {
        errorCode match {
            case 0                                 => "success"
            case ResultCodes.SERVICE_NOT_FOUND     => "service not found"
            case ResultCodes.SERVICE_TIMEOUT       => "service timeout"
            case ResultCodes.SERVICE_BUSY          => "queue is full"
            case ResultCodes.TLV_ENCODE_ERROR      => "parameters encode/decode error"
            case ResultCodes.SERVICE_INTERNALERROR => "service internal error"
            case ResultCodes.SOC_NETWORKERROR      => "network error"
            case -10250013                         => "not supported url"
            case -10250016                         => "server reject"
            case _                                 => "unknown error message " + errorCode
        }
    }

    def jsonTypeProcess(serviceId: Int, msgId: Int, body: HashMapStringAny): HashMapStringAny = {

        val fields = classexMap.getOrElse(serviceId + "-" + msgId, null)
        if (fields == null || fields.size == 0) return body

        val newBody = HashMapStringAny()
        for ((key, value) <- body) {

            if (!fields.contains(key)) {

                newBody.put(key, value)

            } else {

                value match {

                    case s: String =>
                        val v = convertValue(value, key, fields)
                        newBody.put(key, v)

                    case i: Int =>
                        val v = convertValue(value, key, fields)
                        newBody.put(key, v)

                    case l: Long =>
                        val v = convertValue(value, key, fields)
                        newBody.put(key, v)

                    case d: Double =>
                        val v = convertValue(value, key, fields)
                        newBody.put(key, v)

                    case m: HashMapStringAny =>

                        val newMap = convertMap(m, key, fields)
                        newBody.put(key, newMap)

                    case ls: ArrayBufferString =>

                        val buff = ArrayBufferAny()
                        for (value2 <- ls) {
                            val v = convertValue(value2, key, fields)
                            buff += v
                        }
                        newBody.put(key, buff)

                    case li: ArrayBufferInt =>
                        val buff = ArrayBufferAny()
                        for (value2 <- li) {
                            val v = convertValue(value2, key, fields)
                            buff += v
                        }
                        newBody.put(key, buff)

                    case li: ArrayBufferLong =>
                        val buff = ArrayBufferAny()
                        for (value2 <- li) {
                            val v = convertValue(value2, key, fields)
                            buff += v
                        }
                        newBody.put(key, buff)

                    case li: ArrayBufferDouble =>
                        val buff = ArrayBufferAny()
                        for (value2 <- li) {
                            val v = convertValue(value2, key, fields)
                            buff += v
                        }
                        newBody.put(key, buff)

                    case lm: ArrayBufferMap =>
                        val buff = ArrayBufferAny()
                        for (value2 <- lm) {
                            val v = convertMap(value2, key, fields)
                            buff += v
                        }
                        newBody.put(key, buff)

                    case _ =>
                        newBody.put(key, value)
                }

            }

        }

        newBody
    }

    def convertMap(m: HashMapStringAny, key: String, fields: HashMap[String, String]): Any = {
        val newMap = HashMapStringAny()
        for ((key2, value2) <- m) {
            val v = convertValue(value2, key + "-" + key2, fields)
            newMap.put(key2, v)
        }
        newMap
    }

    def convertValue(value: Any, key: String, fields: HashMap[String, String]): Any = {

        if (value == null || value == "" || !fields.contains(key)) {
            return value
        }

        val classex = fields.getOrElse(key, null)
        classex match {
            case "json" =>
                try {
                    return JsonCodec.parseJson(value.toString)
                } catch {
                    case e:Throwable => return value.toString
                }
            case "double" =>
                return value.toString.toDouble
            case "long" =>
                return value.toString.toLong
            case "string" =>
                return value.toString
            case _ =>
                return value
        }

    }

    def strToDate(as: String, defaultValue: Long): Long = {

        var s = as
        if (s == null || s == "") return defaultValue

        if (s.length() == 10) {
            s += " 00:00:00";
        }
        if (s.length() == 13) {
            s += ":00:00";
        }
        if (s.length() == 16) {
            s += ":00";
        }

        var t = 0L

        try {
            t = HttpTimeHelper.convertTimestamp(s).getTime
        } catch {
            case e: Exception =>
                return defaultValue
        }

        if (HttpTimeHelper.convertTimestamp(new Date(t)) != s) {
            return defaultValue
        }

        t
    }

    def toHeaders(httpReq: HttpRequest): String = {
        val headers = httpReq.getHeaders()
        val size = headers.size
        val buff = new StringBuilder()
        for (i <- 0 until size) {
            val entry = headers.get(i)
            if (i > 0) buff.append(",")
            buff.append(entry.getKey).append("=").append(entry.getValue)
        }
        buff.toString
    }

    def convertCharset(v:String,charset:String):String = {
        val bs = v.getBytes("ISO-8859-1")
        new String(bs,charset)
    }

    def parseFileUploadContent2(charset: String,data:String, map: HashMapStringAny) {

        val files = ArrayBufferMap()
        val params = JsonCodec.parseArrayObjectNotNull(data)
        for (m <- params) {
            if (m.contains("filename")) {
                if (m.contains("file")) {
                    val file = m.ns("file")
                    val f = new File(file)
                    if( f.exists() && f.length > 0 ) {
                        val filename = convertCharset( m.ns("filename"), charset )
                        m.put("filename",filename)
                        val p = filename.lastIndexOf(".")
                        if (p > 0) m.put("ext", filename.substring(p).toLowerCase)
                        else m.put("ext", "")
                        m.put("size", f.length)
                        files += m
                    }
                }
            } else {
                val name = m.ns("name") 
                val value = convertCharset( m.ns("value"), charset )
                if (name != "") {
                    map.put(name, value)
                }
            }
        }
        map.put("files", files)
    }

    def parseFileUploadContent(charset: String, in: InputStream, map: HashMapStringAny) {
        var delimeter = parseDelimeter(charset, in)
        if (delimeter == "") return
        val db = delimeter.getBytes()

        var over = false
        val params = ArrayBufferMap()

        while (in.available() > 0 && !over) {
            val m = parsePartAttrs(charset, in)
            if (m.contains("filename")) {
                val t1 = System.currentTimeMillis()
                val (filename, finished) = readMultiPartFile(charset, in, db)
                val t2 = System.currentTimeMillis()
                if (filename != "")
                    m.put("file", filename)
                m.put("parseSeconds", (t2 - t1) / 1000)
                over = finished
            } else {
                val (v, finished) = readMultiPartValue(charset, in, db)
                m.put("value", v)
                over = finished
            }
            params += m
        }
        val files = ArrayBufferMap()
        for (m <- params) {
            if (m.contains("filename")) {
                if (m.contains("file")) {
                    val filename = m.s("filename", "")
                    val p = filename.lastIndexOf(".")
                    if (p > 0) m.put("ext", filename.substring(p).toLowerCase)
                    else m.put("ext", "")

                    val file = m.s("file", "")
                    m.put("size", new File(file).length)

                    files += m
                }
            } else {
                val name = m.s("name", "")
                val value = m.s("value", "")
                if (name != "") {
                    map.put(name, value)
                }
            }
        }
        map.put("files", files)
    }

    def readMultiPartFile(charset: String, in: InputStream, db: Array[Byte]): Tuple2[String, Boolean] = {

        val webappUploadDirExisted = new File(uploadDir).exists()
        if (!webappUploadDirExisted) {
            new File(uploadDir).mkdirs()
        }
        val filename = uploadDir + File.separator + uuid() + ".tmp"

        var writed = 0
        val out = new BufferedOutputStream(new FileOutputStream(filename), 5000000);
        var n = in.read()
        while (n != -1) {
            val b = n.toByte
            if (b == '\r') {
                n = in.read()
                if (n == -1) {
                    out.close()
                    new File(filename).delete()
                    return new Tuple2("", true)
                }
                val b2 = n.toByte
                if (b2 == '\n') {
                    in.mark(db.length + 2)
                    val (matched, finished) = cmp(in, db)
                    if (matched) {
                        out.close()
                        if (writed > 0) {
                            return new Tuple2(filename, finished)
                        } else {
                            new File(filename).delete()
                            return new Tuple2("", finished)
                        }
                    } else {
                        out.write(b)
                        out.write(b2)
                        writed += 2
                        in.reset()
                    }
                } else {
                    out.write(b)
                    out.write(b2)
                    writed += 2
                }
            } else {
                out.write(b)
                writed += 1
            }
            n = in.read()
        }

        out.close()
        new File(filename).delete()
        ("", true)
    }

    def readMultiPartValue(charset: String, in: InputStream, db: Array[Byte]): Tuple2[String, Boolean] = {
        val out = new ByteArrayOutputStream();
        var n = in.read()
        while (n != -1) {
            val b = n.toByte
            if (b == '\r') {
                n = in.read()
                if (n == -1) {
                    return new Tuple2("", true)
                }
                val b2 = n.toByte
                if (b2 == '\n') {
                    in.mark(db.length + 2)
                    val (matched, finished) = cmp(in, db)
                    if (matched) {
                        val v = out.toString(charset)
                        return new Tuple2(v, finished)
                    } else {
                        out.write(b)
                        out.write(b2)
                        in.reset()
                    }
                } else {
                    out.write(b)
                    out.write(b2)
                }
            } else {
                out.write(b)
            }
            n = in.read()
        }

        ("", true)
    }

    def cmp(in: InputStream, db: Array[Byte]): Tuple2[Boolean, Boolean] = {
        var i = 0
        var n = 0
        while (i < db.size) {
            n = in.read()
            if (n == -1) return new Tuple2(false, false)
            val b = n.toByte
            if (b != db(i)) return new Tuple2(false, false)
            i += 1
        }
        n = in.read()
        if (n == -1) return new Tuple2(false, false)
        val b1 = n.toByte
        n = in.read()
        if (n == -1) return new Tuple2(false, false)
        val b2 = n.toByte

        val finished = (b1 == '-' && b2 == '-')
        (true, finished)
    }

    def parseDelimeter(charset: String, in: InputStream): String = {
        val buf = new ByteArrayOutputStream();
        var n = in.read()
        while (n != -1) {
            val b = n.toByte
            buf.write(b)
            if (b == '\n') {
                val line = buf.toString(charset);
                return line.trim
            }
            n = in.read()
        }

        ""
    }

    def parsePartAttrs(charset: String, in: InputStream): HashMapStringAny = {
        val map = HashMapStringAny()
        val str = parsePart(charset, in)
        if (str == "") return map
        val lines = str.split("\r\n")
        for (line <- lines) {
            val p = line.indexOf(":")
            if (p > 0) {
                val key = line.substring(0, p)
                key match {
                    case "Content-Disposition" =>
                        val s = line.substring(p + 1).trim
                        val ss = s.split(";")
                        for (ts <- ss) {
                            val tss = ts.trim.split("=")
                            tss(0) match {
                                case "name" =>
                                    val v = tss(1).replace("\"", "")
                                    map.put("name", v)
                                case "filename" =>
                                    var v = tss(1).replace("\"", "")
                                    val p1 = v.lastIndexOf("/")
                                    if (p1 >= 0) v = v.substring(p1 + 1)
                                    val p2 = v.lastIndexOf("\\")
                                    if (p2 >= 0) v = v.substring(p2 + 1)
                                    map.put("filename", v)
                                case _ =>
                            }
                        }
                    case "Content-Type" =>
                        val contentType = line.substring(p + 1).trim()
                        map.put("contentType", contentType)
                    case _ =>
                }
            }
        }
        map
    }

    def parsePart(charset: String, in: InputStream): String = {
        val buf = new ByteArrayOutputStream();
        var n = in.read()
        while (n != -1) {
            val b = n.toByte
            buf.write(b)
            if (b == '\n') {
                val line = buf.toString(charset)
                if (line.endsWith("\r\n\r\n")) {
                    return line
                }
            }
            n = in.read()
        }

        ""
    }

}

