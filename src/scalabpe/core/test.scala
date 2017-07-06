package scalabpe.core

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.net.URLEncoder
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.reflect.runtime.universe
import scala.xml.XML

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.buffer.DynamicChannelBuffer
import org.jboss.netty.handler.codec.http.CookieDecoder
import org.jboss.netty.handler.codec.http.CookieEncoder
import org.jboss.netty.handler.codec.http.DefaultHttpRequest
import org.jboss.netty.handler.codec.http.HttpMethod
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.handler.codec.http.HttpVersion;

object ValueParser {

    val OP_PREFIX = "___op"
    val NULL = "NULL"

    var debug = false

    var pluginObjectName = "scalabpe.flow.FlowHelper"
    var pluginobj: universe.ModuleMirror = null

    val g_context = HashMapStringAny()

    def initPlugin() {
        val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
        val module = runtimeMirror.staticModule(pluginObjectName)
        pluginobj = runtimeMirror.reflectModule(module)
    }

    // tp: v=value a=array value f=function
    class Field(val key: String, val tp: String, val params: Array[String] = null) {
        override def toString(): String = {
            if (params == null)
                "key=%s,tp=%s,params=null".format(key, tp)
            else
                "key=%s,tp=%s,params=%s".format(key, tp, params.mkString("#"))
        }
    }

    val r1 = """(\$\{[^}]+\})""".r
    val r2 = """(\$[^ ]+)""".r

    // 从全局上下文里解析值
    def parseRightValue(v: Any, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): Any = {
        if (v == null || v == "") return v
        if (!v.isInstanceOf[String]) return v
        val s = v.asInstanceOf[String]
        val nv = ValueParser.parse(s, localCtx, glbCtx, false)

        if (nv == null || nv == "") return nv
        if (!nv.isInstanceOf[String]) return nv
        val t = nv.asInstanceOf[String]

        //if( t.length >= 2 && t.startsWith("\"") &&  t.endsWith("\"") ) return t.substring(1,t.length-1)
        if (t.startsWith("[") && t.endsWith("]")) return JsonCodec.parseArrayNotNull(t)
        if (t.startsWith("{") && t.endsWith("}")) return JsonCodec.parseObjectNotNull(t)
        if (t.startsWith("s:")) return t.substring(2)
        if (t.startsWith("i:")) return t.substring(2).toInt
        t
    }

    // 从invoke结果集或context上下文里解析值
    def parseLeftValue(s: String, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): Any = {
        ValueParser.parse(s, localCtx, glbCtx, true) // 先根据结果集解析, 再根据全局上下文解析
    }

    def anyToString(a: Any): String = {
        if (a == null) return null

        a match {
            case m: HashMapStringAny =>
                JsonCodec.mkString(m)
            case a: ArrayBufferAny =>
                JsonCodec.mkString(a)
            case a: ArrayBufferString =>
                JsonCodec.mkString(a)
            case a: ArrayBufferInt =>
                JsonCodec.mkString(a)
            case a: ArrayBufferLong =>
                JsonCodec.mkString(a)
            case a: ArrayBufferDouble =>
                JsonCodec.mkString(a)
            case a: ArrayBufferMap =>
                JsonCodec.mkString(a)
            case _ =>
                a.toString
        }
    }

    private def parse(s0: String, localCtx: HashMapStringAny, glbCtx: HashMapStringAny, returnNull: Boolean): Any = {
        val s = s0
        var ns = escape4(escape3(escape1(s)))
        if (ns.indexOf("$") >= 0) {
            val vt = parseInternal(s, localCtx, glbCtx)
            if (vt != null && !vt.isInstanceOf[String]) return vt
            if (debug) {
                println("***" + s)
                println("###" + ns)
            }
            ns = r1.replaceAllIn(ns, (m) => parseInternalPart1(m.group(1), localCtx, glbCtx).replace("$", "\\$"))
            if (debug)
                println("@@@" + ns)
            if (ns == null) return null
            ns = r2.replaceAllIn(ns, (m) => parseInternalPart2(m.group(1), localCtx, glbCtx).replace("$", "\\$"))
            if (debug)
                println("%%%" + ns)
            if (ns == null) return null
            return unescape(ns)
        }
        val v = parseInternal(s, localCtx, glbCtx)
        if (v == null && returnNull) return null
        if (v == null && !returnNull) return unescape(escape1(s))
        v match {
            case s: String =>
                unescape(s)
            case _ =>
                v
        }
    }

    def parseInternalPart1(s: String, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): String = {
        var ns = unescape(s)
        ns = "$" + s.substring(2, ns.length - 1)
        val v = parseInternal(ns, localCtx, glbCtx)
        if (v == null) return ""
        v.toString
    }

    def parseInternalPart2(s: String, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): String = {
        var ns = unescape(s)
        val v = parseInternal(ns, localCtx, glbCtx)
        if (v == null) return ""
        v.toString
    }

    def parseInternal(s: String, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): Any = {
        val fields = parseFields(s)
        // println("fields="+fields)            
        if (fields == null) return null

        if (localCtx != null) {
            val v = parseInternal2(fields, localCtx, localCtx, glbCtx)
            if (v != null) return v
        }
        parseInternal2(fields, glbCtx, localCtx, glbCtx)
    }

    def parseInternal2(fields: ArrayBuffer[Field], curCtx: HashMapStringAny, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): Any = {
        var c = curCtx
        var obj: Any = null

        for (i <- 0 until fields.size) {
            val f = fields(i)
            var v: Any = null
            f.tp match {
                case "v" | "a" =>
                    if (c == null) return null // must have a context
                    v = getFieldValue(f, c, localCtx, glbCtx)
                case "f" if c != null =>
                    v = getFieldValue(f, c, localCtx, glbCtx)
                case "f" if obj != null =>
                    v = callObjectFunction(obj, f.key, convertParams(f.params, localCtx, glbCtx))
                case _ =>
                    return null
            }
            if (v == null) return null
            if (i == fields.size - 1) return v
            if (v.isInstanceOf[HashMapStringAny]) {
                c = v.asInstanceOf[HashMapStringAny]
                obj = null
            } else {
                c = null
                obj = v
            }
        }
        null
    }

    def getFieldValue(f: Field, curCtx: HashMapStringAny, localCtx: HashMapStringAny, glbCtx: HashMapStringAny): Any = {
        f.tp match {
            case "v" =>
                curCtx.getOrElse(f.key, null)
            case "a" =>
                getArrayValue(f.key, f.params(0).toInt, curCtx)
            case "f" =>
                if (f.key.startsWith("$"))
                    return callFunction(f.key, convertParams(f.params, localCtx, glbCtx))
                else
                    return callObjectFunction(curCtx, f.key, convertParams(f.params, localCtx, glbCtx))
            case _ =>
                null
        }
    }

    def convertParams(params: Array[String], localCtx: HashMapStringAny, glbCtx: HashMapStringAny): Array[String] = {
        val news = new Array[String](params.length)
        for (i <- 0 until params.size) {
            news(i) = parse(params(i), localCtx, glbCtx, false).toString.trim
        }
        news
    }

    def getArrayValue(key: String, idx: Int, context: HashMapStringAny): Any = {
        val v = context.getOrElse(key, null)
        if (v == null) return null
        v match {
            case a: ArrayBufferString =>
                if (idx < 0 || idx >= a.size) return null
                return a(idx)
            case a: ArrayBufferInt =>
                if (idx < 0 || idx >= a.size) return null
                return a(idx)
            case a: ArrayBufferLong =>
                if (idx < 0 || idx >= a.size) return null
                return a(idx)
            case a: ArrayBufferDouble =>
                if (idx < 0 || idx >= a.size) return null
                return a(idx)
            case a: ArrayBufferMap =>
                if (idx < 0 || idx >= a.size) return null
                return a(idx)
            case a: ArrayBufferAny =>
                if (idx < 0 || idx >= a.size) return null
                return a(idx)
            case _ =>
                return null
        }
    }

    def callFunction(fun: String, params: Array[String]): Any = {
        fun match {
            case "$now" =>
                return now()
            case "$uuid" =>
                return uuid()
            case _ =>
                val methodName = fun.substring(1)
                params.size match {
                    case 0 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName)
                            method.invoke(pluginobj.instance)
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 1 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String])
                            method.invoke(pluginobj.instance, params(0))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 2 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, params(0), params(1))
                        } catch {
                            case e: Throwable =>
                        }
                    case 3 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, params(0), params(1), params(2))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case _ =>
                        null
                }
        }
    }

    def callStringFunction(s: String, fun: String, params: Array[String]): Any = {
        fun match {
            case "size" | "length" =>
                s.length
            case "toString" =>
                s.toString
            case "gt" =>
                if (params.size == 0) return null
                s.toLong > params(0).toLong
            case "ge" =>
                if (params.size == 0) return null
                s.toLong >= params(0).toLong
            case "lt" =>
                if (params.size == 0) return null
                s.toLong < params(0).toLong
            case "le" =>
                if (params.size == 0) return null
                s.toLong <= params(0).toLong
            case "matches" =>
                if (params.size == 0) return null
                s.matches(params(0)).toString
            case "contains" =>
                if (params.size == 0) return null
                (s.indexOf(params(0)) >= 0).toString
            case "indexOf" =>
                if (params.size == 0) return null
                s.indexOf(params(0)).toString
            case "left" =>
                if (params.size == 0) return null
                s.substring(0, params(0).toInt)
            case "right" =>
                if (params.size == 0) return null
                val len = s.length
                if (params(0).toInt > len) return null
                s.substring(len - params(0).toInt)
            case "substring" =>
                if (params.size < 2) return null
                s.substring(params(0).toInt, params(1).toInt)
            case _ =>
                val methodName = fun
                params.size match {
                    case 0 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String])
                            method.invoke(pluginobj.instance, s)
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 1 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, s, params(0))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 2 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, s, params(0), params(1))
                        } catch {
                            case e: Throwable =>
                        }
                    case 3 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[String], classOf[String], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, s, params(0), params(1), params(2))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case _ =>
                        null
                }
        }
    }

    def callMapFunction(m: HashMapStringAny, fun: String, params: Array[String]): Any = {
        fun match {
            case "size"                         => m.size
            case "contains" if params.size == 0 => null
            case "contains" if params.size > 0  => m.contains(params(0)).toString
            case "toString" =>
                m.toString()
            case "toJson" =>
                JsonCodec.mkString(m)
            case _ =>
                val methodName = fun
                params.size match {
                    case 0 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[HashMapStringAny])
                            method.invoke(pluginobj.instance, m)
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 1 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[HashMapStringAny], classOf[String])
                            method.invoke(pluginobj.instance, m, params(0))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 2 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[HashMapStringAny], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, m, params(0), params(1))
                        } catch {
                            case e: Throwable =>
                        }
                    case 3 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[HashMapStringAny], classOf[String], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, m, params(0), params(1), params(2))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case _ =>
                        null
                }
        }
    }
    def callArrayFunction(a: ArrayBufferAny, fun: String, params: Array[String]): Any = {
        fun match {
            case "size"                         => a.size
            case "contains" if params.size == 0 => null
            case "contains" if params.size > 0  => a.contains(params(0)).toString
            case "toString" =>
                a.toString()
            case "toJson" =>
                JsonCodec.mkString(a)
            case _ =>
                val methodName = fun
                params.size match {
                    case 0 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[ArrayBufferAny])
                            method.invoke(pluginobj.instance, a)
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 1 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[ArrayBufferAny], classOf[String])
                            method.invoke(pluginobj.instance, a, params(0))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case 2 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[ArrayBufferAny], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, a, params(0), params(1))
                        } catch {
                            case e: Throwable =>
                        }
                    case 3 =>
                        try {
                            val method = pluginobj.instance.getClass.getMethod(methodName, classOf[ArrayBufferAny], classOf[String], classOf[String], classOf[String])
                            method.invoke(pluginobj.instance, a, params(0), params(1), params(2))
                        } catch {
                            case e: Throwable =>
                                null
                        }
                    case _ =>
                        null
                }
        }
    }

    def callObjectFunction(obj: Any, fun: String, params: Array[String]): Any = {
        obj match {
            case m: HashMapStringAny =>
                callMapFunction(m, fun, params)
            case a: ArrayBufferString =>
                val aa = ArrayBufferAny()
                a.foreach(aa += _)
                callArrayFunction(aa, fun, params)
            case a: ArrayBufferInt =>
                val aa = ArrayBufferAny()
                a.foreach(aa += _.toString)
                callArrayFunction(aa, fun, params)
            case a: ArrayBufferLong =>
                val aa = ArrayBufferAny()
                a.foreach(aa += _.toString)
                callArrayFunction(aa, fun, params)
            case a: ArrayBufferDouble =>
                val aa = ArrayBufferAny()
                a.foreach(aa += _.toString)
                callArrayFunction(aa, fun, params)
            case a: ArrayBufferMap =>
                val aa = ArrayBufferAny()
                a.foreach(aa += _)
                callArrayFunction(aa, fun, params)
            case a: ArrayBufferAny =>
                callArrayFunction(a, fun, params)
            case _ =>
                callStringFunction(obj.toString, fun, params)
        }
    }

    val f0 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def now(): String = {
        f0.format(new java.util.Date())
    }

    def uuid(): String = {
        java.util.UUID.randomUUID().toString().replaceAll("-", "")
    }

    val sepMin = 10001.toChar
    val sep1 = 10001.toChar // . after \
    val sep2 = 10002.toChar // , after \
    val sep3 = 10003.toChar // [ after \
    val sep4 = 10004.toChar // ] after \
    val sep5 = 10005.toChar // ( after \
    val sep6 = 10006.toChar // ) after \
    val sep7 = 10007.toChar // $ after \
    val sep8 = 10008.toChar // { in ()
    val sep9 = 10009.toChar // } in ()
    val sep10 = 10010.toChar // blank in ()
    val sep11 = 10011.toChar // = after \
    val sepMax = 10020.toChar

    def escape(s: String): String = {
        escape3(escape1(s))
    }

    def escape0(s: String): String = {
        var afterSlash = false
        var ts = ""
        for (c <- s) {
            if (afterSlash) {
                c match {
                    case '=' =>
                        ts = ts + sep11
                    case ' ' =>
                        ts = ts + sep10
                    case _ =>
                        ts = ts + '\\'
                        ts = ts + c
                }
                afterSlash = false
            } else if (c == '\\') {
                afterSlash = true
            } else {
                ts = ts + c
            }
        }
        if (afterSlash)
            ts = ts + '\\'
        ts
    }
    def escape1(s: String): String = {
        var afterSlash = false
        var ts = ""
        for (c <- s) {
            if (afterSlash) {
                c match {
                    case '.' =>
                        ts = ts + sep1
                    case ',' =>
                        ts = ts + sep2
                    case '[' =>
                        ts = ts + sep3
                    case ']' =>
                        ts = ts + sep4
                    case '(' =>
                        ts = ts + sep5
                    case ')' =>
                        ts = ts + sep6
                    case '$' =>
                        ts = ts + sep7
                    case '{' =>
                        ts = ts + sep8
                    case '}' =>
                        ts = ts + sep9
                    case '\\' =>
                        ts = ts + '\\'
                    case _ =>
                        ts = ts + '\\'
                        ts = ts + c
                }
                afterSlash = false
            } else if (c == '\\') {
                afterSlash = true
            } else {
                ts = ts + c
            }
        }
        if (afterSlash)
            ts = ts + '\\'
        ts
    }

    def escape3(s: String): String = {
        var brackets = 0
        var ts = ""
        for (c <- s) {

            if (brackets > 0) {
                c match {
                    case '.' =>
                        ts = ts + sep1
                    case ' ' =>
                        ts = ts + sep10
                    case '$' =>
                        ts = ts + sep7
                    case '(' =>
                        brackets += 1
                        ts = ts + sep5
                    case ')' =>
                        brackets -= 1
                        if (brackets < 0) brackets = 0
                        if (brackets == 0)
                            ts = ts + ")"
                        else
                            ts = ts + sep6
                    case _ =>
                        ts = ts + c
                }
            } else if (c == '(') {
                brackets += 1
                ts = ts + "("
            } else {
                ts = ts + c
            }

        }
        ts
    }

    def escape4(s: String): String = {
        var brackets = 0
        var ts = ""
        var lastch = ' '
        for (c <- s) {
            if (brackets > 0) {
                c match {
                    case '$' =>
                        ts = ts + sep7
                    case '{' =>
                        brackets += 1
                        ts = ts + sep8
                    case '}' =>
                        brackets -= 1
                        if (brackets < 0) brackets = 0
                        if (brackets == 0)
                            ts = ts + "}"
                        else
                            ts = ts + sep9
                    case _ =>
                        ts = ts + c
                }
            } else if (c == '{' && lastch == '$') {
                brackets += 1
                ts = ts + "{"
            } else {
                ts = ts + c
            }
            lastch = c
        }
        ts
    }

    def unescape(s: String): String = {
        var found = false
        for (i <- 0 until s.length) {
            val ch = s.charAt(i)
            if (ch >= sep1 && ch <= sep11) found = true
        }
        if (!found) return s

        var ts = ""
        for (ch <- s) {
            ch match {
                case c if c == sep1 =>
                    ts += "."
                case c if c == sep2 =>
                    ts += ","
                case c if c == sep3 =>
                    ts += "["
                case c if c == sep4 =>
                    ts += "]"
                case c if c == sep5 =>
                    ts += "("
                case c if c == sep6 =>
                    ts += ")"
                case c if c == sep7 =>
                    ts += "$"
                case c if c == sep8 =>
                    ts += "{"
                case c if c == sep9 =>
                    ts += "}"
                case c if c == sep10 =>
                    ts += " "
                case c if c == sep11 =>
                    ts += "="
                case c =>
                    ts += c
            }
        }
        ts
    }

    def unescapeAndRemoveQuota(s: String): String = {
        val t = unescape(s.trim())
        t
    }

    def parseFields(s0: String): ArrayBuffer[Field] = {
        val s1 = escape(s0)
        val ss = s1.split("\\.")
        val fields = new ArrayBuffer[Field]()
        for (s <- ss) {
            val f = parseSingleField(s)
            if (f == null) return null
            fields += f
        }
        if (fields.size == 0) return null
        fields
    }
    def parseSingleField(s: String): Field = {
        val p01 = s.indexOf("(")
        val p02 = s.indexOf("[")
        val f = (p01, p02) match {
            case (-1, -1) =>
                new Field(unescapeAndRemoveQuota(s), "v")
            case (p1, -1) =>
                parseFunctionField(s)
            case (-1, p1) =>
                parseArrayField(s)
            case (p1, p2) if p1 < p2 =>
                parseFunctionField(s)
            case (p1, p2) if p1 > p2 =>
                parseArrayField(s)
            case _ =>
                null
        }
        if (f == null)
            new Field(unescapeAndRemoveQuota(s), "v")
        else
            f
    }

    def parseArrayField(s: String): Field = {
        val p1 = s.indexOf("[")
        if (p1 == 0) return null
        if (!s.trim.endsWith("]")) return null
        val key = unescapeAndRemoveQuota(s.substring(0, p1).trim)
        val p2 = s.lastIndexOf("]")
        val ps = s.substring(p1 + 1, p2).trim
        if (!isInt(ps)) return null
        if (ps.toInt < 0) return null
        val f = new Field(key, "a", Array[String](ps))
        return f
    }
    def parseFunctionField(s: String): Field = {
        val p1 = s.indexOf("(")
        if (p1 == 0) return null
        if (!s.trim.endsWith(")")) return null
        val key = unescapeAndRemoveQuota(s.substring(0, p1).trim)
        val p2 = s.lastIndexOf(")")
        val ps = s.substring(p1 + 1, p2).trim
        var params = ps.split(",").map(_.trim)
        if (params.size == 1 && params(0) == "") params = Array[String]()
        for (i <- 0 until params.size) params(i) = unescapeAndRemoveQuota(params(i))
        val f = new Field(key, "f", params)
        return f
    }

    def isInt(n: String): Boolean = {
        try {
            Integer.parseInt(n)
            return true
        } catch {
            case e: Throwable =>
                return false
        }
    }
}

class MockActor extends Actor with Logging with SyncedActor {

    val retmap = new ConcurrentHashMap[String, Response]()
    override def receive(v: Any): Unit = {
        v match {
            case req: Request =>

                val buff = Router.main.mocks.getOrElse(req.serviceId + ":" + req.msgId, null)
                if (buff == null) {
                    reply(req, -10242504)
                    return
                }

                genResponse(req, buff)

            case _ =>
                log.error("unknown msg")
        }
    }

    def checkMatch(req: Request, cfg: MockCfg, context: HashMapStringAny): Boolean = {
        for ((k, v) <- cfg.req if !k.endsWith(ValueParser.OP_PREFIX)) {
            val left = ValueParser.anyToString(ValueParser.parseLeftValue(k, req.body, context))
            val right = ValueParser.anyToString(ValueParser.parseRightValue(v, null, context))
            val op = cfg.req.getOrElse(k + ValueParser.OP_PREFIX, "=")

            if (op == "=") {
                if (right == ValueParser.NULL && left != null)
                    return false

                if (right != ValueParser.NULL && left != right)
                    return false
            }

            if (op == "!=") {
                if (right == ValueParser.NULL && left == null)
                    return false

                if (right != ValueParser.NULL && left == right)
                    return false
            }

        }
        true
    }

    def getCfg(req: Request, cfgs: ArrayBuffer[MockCfg], context: HashMapStringAny): MockCfg = {
        for (cfg <- cfgs) {
            if (cfg.req.size == 0) return cfg
            if (checkMatch(req, cfg, context)) return cfg
        }
        null
    }

    def genResponse(req: Request, cfgs: ArrayBuffer[MockCfg]) {

        val context = HashMapStringAny()
        context ++= ValueParser.g_context
        context.put("$this", HashMapStringAny("req" -> req.body))

        val cfg = getCfg(req, cfgs, context)
        if (cfg == null) {
            reply(req, -10242404)
            return
        }
        val code = cfg.res.i("$code")
        val params = HashMapStringAny()
        for ((k, v) <- cfg.res) {
            val v2 = ValueParser.parseRightValue(v, null, context)
            params.put(k, v2)
        }
        if (code != 0) {
            reply(req, code, params)
            return
        }
        reply(req, 0, params)
    }

    def reply(req: Request, code: Int): Unit = {
        reply(req, code, new HashMapStringAny())
    }

    def reply(req: Request, code: Int, params: HashMapStringAny): Unit = {
        val (newbody, ec) = Router.main.encodeResponse(req.serviceId, req.msgId, code, params)
        var errorCode = code
        if (errorCode == 0 && ec != 0) {
            errorCode = ec
        }

        val res = new Response(errorCode, newbody, req)
        put(res.requestId, res)
    }

    def get(requestId: String): Response = {
        retmap.remove(requestId)
    }

    def put(requestId: String, ret: Response) {
        retmap.put(requestId, ret)
    }

}

trait TestCaseV2Command {
    def toString(indent: String): String
}

class TestCaseV2Define(val defines: LinkedHashMapStringAny) extends TestCaseV2Command {
    var lineNo = 0
    def toString(m: LinkedHashMapStringAny): String = {
        val b = new StringBuilder()
        for ((k, v) <- m) {
            b.append(" ").append(k).append("=").append(v)
        }
        b.toString
    }
    def toString(indent: String): String = {
        var s = indent + "define: %s".format(toString(defines))
        s
    }
}
class TestCaseV2Invoke(val tp: String, val service: String, val timeout: Int, val req: LinkedHashMapStringAny, val res: LinkedHashMapStringAny, val id: String = "") extends TestCaseV2Command {
    var lineNo = 0

    var http_method = ""
    var http_rest = ""
    var http_ok_field = ""
    var http_ok_value = ""

    def toString(m: LinkedHashMapStringAny): String = {
        val b = new StringBuilder()
        for ((k, v) <- m if !k.endsWith(ValueParser.OP_PREFIX)) {
            val op = m.getOrElse(k + ValueParser.OP_PREFIX, "=").toString
            b.append(" ").append(k).append(op).append(v)
        }
        b.toString
    }
    def toString(indent: String): String = {
        var s = indent + "%s:%s id:%s timeout:%d req:%s res:%s".format(tp, service, id, timeout, toString(req), toString(res))
        s = s.replace(" timeout:15000 ", " ")
        s = s.replace(" id: ", " ")
        s
    }
}

class TestCaseV2(val tp: String, val name: String, val commands: ArrayBuffer[TestCaseV2Command]) {

    var lineNo = 0
    var enabled = true
    var extendsFrom = ""
    var pluginObjectName = ""
    var remote = ""

    var http_base_url = ""
    var http_method = ""
    var http_rest = ""
    var http_ok_field = ""
    var http_ok_value = ""

    override def toString(): String = {

        val indent = "    "

        val buff = ArrayBufferString()

        if (name == "global")
            buff += "global:"
        else
            buff += "testcase:" + name + " enabled:" + enabled + " extends:" + extendsFrom

        if (commands != null)
            commands.foreach(buff += _.toString(indent))

        buff.mkString("\n")
    }
}

object TestCaseRunner extends Logging {

    val indent = "    "
    val codeTag = "$code"
    val savedMocks = HashMap[String, ArrayBuffer[MockCfg]]()
    var sequence = new AtomicInteger(1)
    val lock = new ReentrantLock(false)
    val replied = lock.newCondition()
    var testCaseCount = 0
    var ir: InvokeResult = _

    var runAll = false

    var remote = ""

    var g_http_base_url = ""
    var g_http_method = ""
    var g_http_rest = ""
    var g_http_ok_field = ""
    var g_http_ok_value = ""
    var g_cookies = HashMapStringString()

    val timeout = 15000
    var codecs: TlvCodecs = _
    var soc: SocImpl = _
    var remoteReqRes: SocRequestResponseInfo = _

    var hasError = false
    var total = 0
    var success = 0
    var failed = 0

    var allfiles_total = 0
    var allfiles_success = 0
    var allfiles_failed = 0

    var lineNoCnt = 0
    var httpClient = new RunTestHttpClient()

    object TestActor extends Actor {
        def receive(v: Any) {
            v match {
                case res: InvokeResult =>
                    lock.lock();
                    try {
                        ir = res
                        replied.signal()
                    } finally {
                        lock.unlock();
                    }
                case _ =>
                    println("unknown msg")
            }
        }
    }

    // remote 调用使用
    def socCallback(any: Any) {

        any match {

            case reqRes: SocRequestResponseInfo =>

                lock.lock();
                try {
                    remoteReqRes = any.asInstanceOf[SocRequestResponseInfo]
                    replied.signal()
                } finally {
                    lock.unlock();
                }

            case ackInfo: SocRequestAckInfo =>

                println("ack=" + ackInfo.req.requestId)

            case _ =>
        }
    }

    def parseFiles(args: Array[String]): ArrayBufferString = {
        var i = 0
        val buff = new ArrayBufferString()
        while (i < args.size) {
            if (args(i).startsWith("--")) i += 2
            else if (args(i).startsWith("-")) i += 1
            else { buff += args(i); i += 1; }
        }
        buff
    }

    def help() {
        println(
            """
usage: scalabpe.core.TestCaseRunner [options] path_to_testcasefile(txt)
options:
    -h|--help             帮助信息
    -a|--all              忽略enabled标志运行所有testcase
       --all_files        对testcase下所有txt文件运行测试
    -d|--dump             输出解析后的testcase到控制台
""")
    }

    def parseArgs(args: Array[String]): HashMapStringAny = {
        val map = HashMapStringAny()
        var i = 0
        val files = ArrayBufferString()
        while (i < args.size) {
            args(i) match {
                case "-h" | "--help" =>
                    return null
                case "-a" | "--all" =>
                    map.put("all", "1")
                    i += 1
                case "--all_files" =>
                    map.put("all_files", "1")
                    i += 1
                case "-c" | "--cover" =>
                    map.put("cover", "1")
                    i += 1
                case "-d" | "--dump" =>
                    map.put("dump", "1")
                    i += 1
                case "--debug" =>
                    map.put("debug", "1")
                    i += 1
                case s if s.startsWith("-") =>
                    println("invalid option " + s)
                    return null
                case _ =>
                    files += args(i)
                    i += 1
            }
        }
        map.put("files", files)
        map
    }

    def matchFile(file: String, patterns: ArrayBufferString): Boolean = {
        for (p <- patterns) {
            if (file.matches(p)) return true
            if (file.matches("testcase/" + p)) return true
        }
        false
    }

    def main(args: Array[String]) {

        var params = parseArgs(args)
        if (params == null) {
            help()
            return
        }

        var files = params.nls("files")

        if (files.length == 0) {
            files += "default.txt"
        }

        if (params.ns("all_files") == "1") {
            files.clear()
            for (f <- new File("./testcase/").listFiles) {
                if (f.getName().endsWith(".txt")) {
                    files += f.getName()
                } else if (f.isDirectory) {
                    for (f2 <- new File("./testcase/" + f.getName()).listFiles if f2.getName().endsWith(".txt")) {
                        files += f.getName() + "/" + f2.getName()
                    }
                }
            }
        }

        if (files.filter(_.indexOf("*") > 0).size > 0) { // support xxx/*.txt
            val patternfiles = ArrayBufferString()
            patternfiles ++= files
            for (i <- 0 until patternfiles.size) {
                patternfiles(i) = patternfiles(i).replace("\\", "/").replace(".", "\\.").replace("*", ".*")
            }
            files.clear()
            val allfiles = ArrayBufferString()
            for (f <- new File("./testcase/").listFiles) {
                if (f.getName().endsWith(".txt")) {
                    allfiles += f.getName()
                } else if (f.isDirectory) {
                    for (f2 <- new File("./testcase/" + f.getName()).listFiles if f2.getName().endsWith(".txt")) {
                        allfiles += f.getName() + "/" + f2.getName()
                    }
                }
            }

            for (i <- 0 until allfiles.size) {
                if (matchFile(allfiles(i), patternfiles)) {
                    files += allfiles(i)
                }
            }
        }
        if (true) {
            println("testcase files:" + files.mkString(","))
        }
        try {
            for (f <- files) {
                runTest(f, params, args)
            }

            if (Router.testMode) {
                Main.close()
            }
            if (soc != null) {
                soc.close()
                log.asInstanceOf[ch.qos.logback.classic.Logger].getLoggerContext().stop
                soc = null
            }
        } catch {
            case e: Throwable =>
                log.error("exception e=" + e.getMessage, e)
                e.printStackTrace()
                hasError = true
        }

        httpClient.close()

        println("-------------------------------------------")
        println("all testcases, total:%d, success:%d, failed:%d".format(allfiles_total, allfiles_success, allfiles_failed))
        println("-------------------------------------------")

        if (params.ns("cover") == "1") {
            generateCoverageReport()
        }

        if (hasError) System.exit(1)
        else System.exit(0)
    }

    // clsName 非空表示这一行是cls定义, methodName 非空表示这一行是method定义
    class FlowCallLine(val lineNumber: Int, val lineText: String, val clsName:String, val methodName:String) {
        var called = false
    }
    
    class FlowInfo(val clsName: String, val filename: String, val path: String, val lines: ArrayBuffer[FlowCallLine]) {
        
        def called(methodName:String,lineNumber:Int) {
            for( l <- lines ) {
                if( l.clsName == clsName ) l.called = true
                if( l.lineNumber == lineNumber ) l.called = true
                if( l.methodName == methodName ) l.called = true
            }
        }

        def total(): Int = {
            lines.size 
        }
        def tested(): Int = {
            lines.filter(_.called == true).size 
        }

        def toCoverage(): String = {
            val tt = total()
            val te = tested()
            val buff = ArrayBufferString()
            val s = "coverage, file:%s total:%d, tested:%d%%".format(path, tt, te*100/tt)
            buff += s
            if( te > 0 && te != tt ) {
                for (fi <- lines if !fi.called  ) {
                    val s2 = "+++++++++ not tested, #%d   %s".format(fi.lineNumber,fi.lineText)
                    buff += s2
                }
            }
            return buff.mkString("\n")            
        }
    }

    def generateCoverageReport() {

        // .flow.scala后缀的流程：scalabpe.flow.Flow_service999_test222,receive,test222.flow.scala,14
        // .flow后缀的流程  scalabpe.flow.Flow_service999_echo,receive,compose_conf_simpleflows_service999_echo.scala,5

        val flows = scanFlows()
        val scalaFlows = scanScalaFlows()

        val all_flows = HashMap[String, FlowInfo]()

        for (i <- flows) {
            all_flows.put(i.clsName, i)
        }
        for (i <- scalaFlows) {
            all_flows.put(i.clsName, i)
        }

        val it = Flow.callStats.values().iterator()
        while (it.hasNext()) {
            val v = it.next()
            val ss = v.split(":")
            val clsName = ss(0)
            val methodName = ss(1)
            val fileName = ss(2)
            val lineNumber = ss(3).toInt
//println(v)
            val fi = all_flows.getOrElse(clsName, null)
            if (fi != null) {
                fi.called(methodName,lineNumber)
            }
        }

        var tt = 0
        var te = 0 
        for (f <- flows) {
            tt += f.total()
            te += f.tested()
            println(f.toCoverage())
        }
        for (f <- scalaFlows) {
            tt += f.total()
            te += f.tested()
            println(f.toCoverage())
        }
        val s = "all files coverage, total:%d, tested:%d percent:%d%%".format(tt, te, te*100/tt)
        println(s)
        println("-------------------------------------------")
    }

    def listfiles(dir: String, suffix: String): List[String] = {
        var dirs = ArrayBuffer[File](new File(dir))
        var newdirs = ArrayBuffer[File]()
        val files = ArrayBuffer[File]()

        while (dirs.size > 0) {
            for (d <- dirs) {
                val fs = d.listFiles()
                fs.filter(f => !f.isDirectory).foreach(files += _)
                fs.filter(f => f.isDirectory).foreach(newdirs += _)
            }
            dirs = newdirs
            newdirs = ArrayBuffer[File]()
        }
        val allfiles = files.map(_.getPath).filter(name => name.endsWith(suffix)).toList
        allfiles
    }

    def scanFlows(): ArrayBuffer[FlowInfo] = {
        val composeDir = Router.main.rootDir + File.separator + "compose_conf"
        val allfiles = listfiles(composeDir, ".flow")
        val list = ArrayBuffer[FlowInfo]()
        for (f <- allfiles) {
            val fi = parseFlowFile(f, scalaFlow = false)
            if (fi != null)
                list += fi
        }
        return list
    }

    def scanScalaFlows(): ArrayBuffer[FlowInfo] = {
        val composeDir = Router.main.rootDir + File.separator + "compose_conf"
        val allfiles = listfiles(composeDir, ".flow.scala")
        val list = ArrayBuffer[FlowInfo]()
        for (f <- allfiles) {
            val fi = parseFlowFile(f, scalaFlow = true)
            if (fi != null)
                list += fi
        }
        return list
    }

    // 静态解析的文件内容： 类名信息，invoke行, reply行, 回调函数行(不包括receive)
    def parseFlowFile(f: String, scalaFlow: Boolean): FlowInfo = {
        val lines = Source.fromFile(f, "UTF-8").getLines.toBuffer
        val clsName = parseClsName(lines, scalaFlow)
        if (clsName == null) return null
        val callLines = parseCallLines(lines, scalaFlow)
        val filename = parseFileName(f)
        val flow = new FlowInfo(clsName, filename, f, callLines)
        flow
    }

    def parseFileName(f: String): String = {
        val s = f.replace("\\", "/")
        val p = s.lastIndexOf("/")
        if (p >= 0) return s.substring(p + 1)
        s
    }

    def parseClsName(lines: Buffer[String], scalaFlow: Boolean): String = {
        if (scalaFlow) {
            val t = lines.filter(_.indexOf("class Flow_") == 0)
            if (t.size == 0) return null
            val line = t(0)
            val p1 = line.indexOf(" ")
            val p2 = line.indexOf(" ", p1 + 1)
            val name = line.substring(p1 + 1, p2)
            return "scalabpe.flow." + name
        } else {
            val t = lines.filter(s => s.toLowerCase().trim().indexOf("//$") == 0)
            if (t.size == 0) return null
            val line = t(0).trim
            var name = ""
            var p = line.indexOf(".with")
            if (p >= 0) {
                name = line.substring(3, p)
            } else {
                p = line.indexOf(" ")
                if (p >= 0) {
                    name = line.substring(3, p)
                } else {
                    name = line.substring(3)
                }
            }

            return "scalabpe.flow.Flow_" + name.replace(".", "_")
        }

    }
    def parseCallLines(lines: Buffer[String], scalaFlow: Boolean): ArrayBuffer[FlowCallLine] = {
        val callLines = ArrayBuffer[FlowCallLine]()
        var i = 1
        for (l <- lines) {
            val fcl = parseLine(l, i)
            if (fcl != null) callLines += fcl
            i += 1
        }
        callLines
    }

    val pattern_cls1 = """^class +([a-zA-Z0-9_]+) .*$""".r
    val pattern_cls2 = """^//$([a-zA-Z0-9_.]+).*$""".r
    
    val pattern_def11 = """^def +([a-zA-Z0-9_]+)\(\).*$""".r
    val pattern_def12 = """^override +def ([a-zA-Z0-9_]+)\(\).*$""".r
    val pattern_def2 = """^//#([a-zA-Z0-9_]+).*$""".r
    
    val pattern_reply = """^reply.*$""".r

    val pattern_invoke = """^invoke.*$""".r
    val pattern_invokeFuture = """^.*invokeFuture.*$""".r
    val pattern_syncedInvoke = """^syncedInvoke.*$""".r

    def parseLine(line: String, lineNumber: Int): FlowCallLine = {
        val (ok,clsName,methodName) = line.trim match {
            case pattern_cls1(clsName)     => 
                (true,"scalabpe.flow."+clsName,"")
            case pattern_cls2(clsName)     => 
                (true,"scalabpe.flow."+clsName,"")
            case pattern_def11(methodName)     => (true,"",methodName)
            case pattern_def12(methodName)     => (true,"",methodName)
            case pattern_def2(methodName)     => (true,"",methodName)
            case pattern_reply()    => (true,"","")
            case pattern_invokeFuture()   => 
                if( line.indexOf("invoke2") >= 0 ||
                    line.indexOf("invoke3") >= 0 ||
                    line.indexOf("invoke4") >= 0 ||
                    line.indexOf("invoke5") >= 0 ||
                    line.indexOf("invokeMulti") >= 0  ) 
                    (true,"","")
                else    
                    (false,"","")
            case pattern_invoke()   => (true,"","")
            case pattern_syncedInvoke()   => (true,"","")
            case _                  => (false,"","")
        }
        if (ok) new FlowCallLine(lineNumber, line, clsName, methodName)
        else null

    }

    def resetGlobal() {
        ValueParser.pluginObjectName = "scalabpe.flow.FlowHelper"
        ValueParser.pluginobj = null
        ValueParser.g_context.clear()

        savedMocks.clear()
        testCaseCount = 0
        total = 0
        success = 0
        failed = 0
        lineNoCnt = 0
    }

    // called only by scalabpe.core.main, 在正常启动时安装mock
    def installMock(f: String) {
        try {
            installMockInternal(f)
        } catch {
            case e: Throwable =>
                log.error("install mock file exception, file=" + f + ", message=" + e.getMessage)
        }
    }

    def installMockInternal(f: String) {
        var file = f
        if (!new File(file).exists()) {
            log.error("mock file not found, file=" + f)
            return
        }

        val lines = Source.fromFile(file, "UTF-8").getLines.toBuffer.map((s: String) => s.trim).map(appendLineNo).map(removeComment).map(_.replace("\t", " ")).map(_.trim).filter(_.trim != "")
        val mergedlines2 = mergeLines(lines).filter(!_.startsWith("#"))
        val mergedlines = ArrayBufferString()
        mergedlines2.foreach(mergedlines += _)
        val (global, dummy) = parseFile(mergedlines)
        if (global == null) {
            log.info("no mock need to be installed, file=" + f)
            return
        }
        Router.main.mockActor = new MockActor()
        if (global.pluginObjectName != "")
            ValueParser.pluginObjectName = global.pluginObjectName
        ValueParser.initPlugin()
        val context = ValueParser.g_context
        if (global.commands != null) {
            for (c <- global.commands) {
                c match {
                    case d: TestCaseV2Define =>
                        installDefines(d, context)
                    case i: TestCaseV2Invoke =>
                        i.tp match {
                            case "mock" =>
                                val (ok, msg) = installMock(i)
                                if (!ok) {
                                    log.error("global mock install failed! service=" + i.service + ", reason=" + msg)
                                    return
                                }
                            case _ => // ignore others
                        }
                }
            }
        }
        log.info("mock installed, file=" + f)
    }

    def runTest(f: String, params: HashMapStringAny, args: Array[String]) {
        var file = f
        if (!new File(file).exists()) {
            file = "." + File.separator + "testcase" + File.separator + f
            if (!new File(file).exists()) {
                println("testcase file not found, file=" + f)
                help()
                return
            }
        }
        file = file.replace("\\", "/")

        if (!isNewFormat(file)) {
            TestCaseRunnerV1.main(args)
            return
        }

        resetGlobal()

        var dumpFlag = params.ns("dump") == "1"
        runAll = params.ns("all") == "1"
        ValueParser.debug = params.ns("debug") == "1"

        val lines = Source.fromFile(file, "UTF-8").getLines.toBuffer.map((s: String) => s.trim).map(appendLineNo).map(removeComment).map(_.replace("\t", " ")).map(_.trim).filter(_.trim != "")
        val mergedlines2 = mergeLines(lines).filter(!_.startsWith("#"))
        val mergedlines = ArrayBufferString()
        mergedlines2.foreach(mergedlines += _)
        val (global, testcases) = parseFile(mergedlines)
        if (testcases == null) {
            return
        }
        if (global != null && global.remote != "") {
            remote = global.remote
            if (remote != "0" && remote.indexOf(":") < 0) {
                remote = TestCaseRunnerV1.loadTestServerAddr()
            }
            codecs = new TlvCodecs("." + File.separator + "avenue_conf")
            soc = new SocImpl(remote, codecs, socCallback, connSizePerAddr = 1)
        }
        g_cookies.clear()
        if (global != null) {
            g_http_base_url = global.http_base_url
            g_http_method = global.http_method
            g_http_rest = global.http_rest
            g_http_ok_field = global.http_ok_field
            g_http_ok_value = global.http_ok_value
        } else {
            g_http_base_url = ""
            g_http_method = ""
            g_http_rest = ""
            g_http_ok_field = ""
            g_http_ok_value = ""
        }

        if (!Router.testMode && remote == "") {
            Router.testMode = true
            Main.main(Array[String]())
            Router.main.mockActor = new MockActor()
        }

        println("###########################################")
        println("testcase file:  " + file)

        if (dumpFlag) {
            println("-------------------------------------------")
            dump(global, testcases)
        }

        println("-------------------------------------------")
        try {
            runTest(global, testcases)
        } catch {
            case e: Throwable =>
                if (e.getMessage() == "stop")
                    println(">>> stop --- interrupted! ---")
                else
                    throw e
        }

        if (failed > 0) hasError = true

        allfiles_total += total
        allfiles_success += success
        allfiles_failed += failed

        println("-------------------------------------------")
        println("testcase result, file:%s total:%d, success:%d, failed:%d".format(file, total, success, failed))
        println("-------------------------------------------")
    }

    def runTest(global: TestCaseV2, testcases: ArrayBuffer[TestCaseV2]) {

        if (global != null && global.pluginObjectName != "")
            ValueParser.pluginObjectName = global.pluginObjectName
        ValueParser.initPlugin()

        val context = ValueParser.g_context
        context.clear()

        if (remote == "")
            Router.main.mocks.clear()

        if (global != null && global.commands != null) {
            for (c <- global.commands) {
                c match {
                    case d: TestCaseV2Define =>
                        installDefines(d, context)
                    case i: TestCaseV2Invoke =>
                        i.tp match {
                            case "mock" =>
                                val (ok, msg) = installMock(i)
                                if (!ok) {
                                    println(">>> LINE#" + i.lineNo + " " + i.toString(""))
                                    println("<<< global mock install failed, stop test! service=" + i.service + ", reason=" + msg)
                                    return
                                }
                            case "setup" | "setup_http" =>
                                val (ok, msg, req, res) = callServiceMustOk(i, context)
                                if (!ok) {
                                    println(">>> LINE#" + i.lineNo + " " + i.toString(""))
                                    println(">>> " + req)
                                    println("<<< " + res)
                                    println("<<< global setup failed, stop test! service=" + i.service + ", reason=" + msg)
                                    return
                                }
                            case _ => // ignore teardown and assert
                        }
                }
            }
        }

        saveGlobalMock()

        if (testcases != null) {
            for (t <- testcases) {
                doTest(t, context, testcases)
            }
        }

        if (global != null && global.commands != null) {
            for (c <- global.commands) {
                c match {
                    case i: TestCaseV2Invoke =>
                        i.tp match {
                            case "teardown" | "teardown_http" =>
                                callServiceIgnoreResult(i, context)
                            case _ => // ignore mock,setup,assert
                        }
                    case _ => // ignore define
                }
            }
        }

    }

    def getBaseTestCase(from: String, testcases: ArrayBuffer[TestCaseV2]): TestCaseV2 = {
        for (t <- testcases) {
            if (t.name == from) {
                return t
            }
        }
        null
    }

    // 只支持extends一级,不支持更多级
    def doTest(t: TestCaseV2, context: HashMapStringAny, testcases: ArrayBuffer[TestCaseV2]) {
        if (!t.enabled && !runAll) return

        if (remote == "") {
            Router.main.mocks.clear()
            Router.main.mocks ++= savedMocks
        }

        var base: TestCaseV2 = null
        if (t.extendsFrom != "") {
            base = getBaseTestCase(t.extendsFrom, testcases)
            if (base == null) {
                println("<<< testcase extends not found, testcase=%s, extends=%s".format(t.name, t.extendsFrom))
                return
            }
            doTestStep1(base, context, testcases)
        }
        doTestStep1(t, context, testcases)

        if (base != null) {
            doTestStep2(base, context, testcases)
        }
        doTestStep2(t, context, testcases)
    }

    def doTestStep1(t: TestCaseV2, context: HashMapStringAny, testcases: ArrayBuffer[TestCaseV2]) {

        if (t != null && t.commands != null) {
            for (c <- t.commands) {
                c match {
                    case d: TestCaseV2Define =>
                        installDefines(d, context)
                    case i: TestCaseV2Invoke =>
                        i.tp match {
                            case "mock" =>
                                val (ok, msg) = installMock(i)
                                if (!ok) {
                                    failed += 1
                                    total += 1
                                    println(">>> LINE#" + i.lineNo + " " + i.toString(""))
                                    println("<<< testcase mock failed, testcase=%s, service=%s, reason=%s".format(t.name, i.service, msg))
                                    return
                                }
                            case "setup" | "setup_http" =>
                                val (ok, msg, req, res) = callServiceMustOk(i, context)
                                if (!ok) {
                                    failed += 1
                                    total += 1
                                    println(">>> LINE#" + i.lineNo + " " + i.toString(""))
                                    println(">>> " + req)
                                    println("<<< " + res)
                                    println("<<< testcase setup failed, testcase=%s, service=%s, reason=%s".format(t.name, i.service, msg))
                                    return
                                }
                            case "assert" | "assert_http" =>
                                val (ok, msg, req, res) = callServiceWithAssert(i, context)
                                if (!ok) {
                                    failed += 1
                                    total += 1
                                    println(">>> LINE#" + i.lineNo + " " + i.toString(""))
                                    println(">>> " + req)
                                    println("<<< " + res)
                                    println("<<< assert failed, testcase=%s, service=%s, reason=%s".format(t.name, i.service, msg))
                                    println("-------------------------------------------")
                                } else {
                                    success += 1
                                    total += 1
                                }
                            case _ => // ignore teardown 
                        }
                }
            }
        }

    }

    def doTestStep2(t: TestCaseV2, context: HashMapStringAny, testcases: ArrayBuffer[TestCaseV2]) {
        if (t != null && t.commands != null) {
            for (c <- t.commands) {
                c match {
                    case i: TestCaseV2Invoke =>
                        i.tp match {
                            case "teardown" | "teardown_http" =>
                                callServiceIgnoreResult(i, context)
                            case _ => // ignore mock,setup,assert
                        }
                    case _ => // ignore define
                }
            }
        }
    }

    def callServiceIgnoreResult(i: TestCaseV2Invoke, context: HashMapStringAny) {
        callService(i, context)
    }
    def callServiceMustOk(i: TestCaseV2Invoke, context: HashMapStringAny): Tuple4[Boolean, String, String, String] = {
        callServiceWithAssert(i, context)
    }
    def callServiceWithAssert(i: TestCaseV2Invoke, context: HashMapStringAny): Tuple4[Boolean, String, String, String] = {
        val (req, ret) = callService(i, context)
        for ((k, v) <- i.res if !k.endsWith(ValueParser.OP_PREFIX)) {

            val left = ValueParser.anyToString(ValueParser.parseLeftValue(k, ret, context))
            val right = ValueParser.anyToString(ValueParser.parseRightValue(v, null, context))
            val op = i.res.getOrElse(k + ValueParser.OP_PREFIX, "=")
            if (op == "=") {
                if (right == ValueParser.NULL && left != null)
                    return (false, "[" + k + "] not match, required:null, actual:not null", req.toString, ret.toString)

                if (right != ValueParser.NULL && left != right)
                    return (false, "[" + k + "] not match, required:" + right + ", actual:" + left, req.toString, ret.toString)
            }
            if (op == "!=") {
                if (right == ValueParser.NULL && left == null)
                    return (false, "[" + k + "] not match, required:not null, actual:null", req.toString, ret.toString)

                if (right != ValueParser.NULL && left == right)
                    return (false, "[" + k + "] not match, required:!=" + right + ", actual:" + left, req.toString, ret.toString)
            }

        }
        (true, "success", req.toString, ret.toString)
    }

    def generateSequence(): Int = {
        sequence.getAndIncrement()
    }

    def saveInvokeToContext(id: String, tp: Tuple2[HashMapStringAny, HashMapStringAny], context: HashMapStringAny) {
        if (id == "") return
        val map = HashMapStringAny("req" -> tp._1, "res" -> tp._2)
        context.put(id, map)
        context.put("$this", map)
    }

    def callService(i: TestCaseV2Invoke, context: HashMapStringAny): Tuple2[HashMapStringAny, HashMapStringAny] = {
        val params = HashMapStringAny()
        for ((k, v) <- i.req) {
            params.put(k, ValueParser.parseRightValue(v, null, context))
        }
        if (i.service.toLowerCase == "sleep") {
            val s = params.i("s")
            val ms = params.i("ms")
            val t = if (s == 0) ms else s * 1000
            val m = params.ns("m")
            if (m != "") println(">>> sleeping --- " + m)
            Thread.sleep(t)
            val tp = (params, HashMapStringAny("$code" -> 0))
            saveInvokeToContext(i.id, tp, context)
            return tp
        }
        if (i.service.toLowerCase == "echo") {
            val m = params.ns("m")
            if (m != "") println(">>> echo --- " + m)
            val tp = (params, HashMapStringAny("$code" -> 0, "m" -> m))
            saveInvokeToContext(i.id, tp, context)
            return tp
        }
        if (i.service.toLowerCase == "stop") {
            throw new Exception("stop")
        }

        var req_body = HashMapStringAny()
        var res_body = HashMapStringAny()

        if (i.tp.endsWith("_http")) {
            var url = ValueParser.parseRightValue(i.service, null, context).toString
            if (!url.startsWith("http://") && g_http_base_url != "") {
                url = g_http_base_url + url
            }
            var http_method = i.http_method
            if (http_method == "") http_method = g_http_method
            if (http_method == "") http_method = "post"

            var http_rest = i.http_rest
            if (http_rest == "") http_rest = g_http_rest
            if (http_rest == "") http_rest = "false"

            var http_ok_field = i.http_ok_field
            if (http_ok_field == "") http_ok_field = g_http_ok_field
            if (http_ok_field == "") http_ok_field = "return_code"

            var http_ok_value = i.http_ok_value
            if (http_ok_value == "") http_ok_value = g_http_ok_value
            if (http_ok_value == "") http_ok_value = "0"

            val map = HashMapStringAny()
            req_body = params
            res_body = map

            val requestId = generateSequence().toString
            req_body.put("$requestId", requestId)

            val header = HashMapStringString()
            val body = HashMapStringAny()
            for ((k, v) <- params if k.startsWith("header.")) {
                header.put(k.substring(7), v.toString)
            }
            for ((k, v) <- params if !k.startsWith("header.")) {
                body.put(k, v)
            }

            val (code, res) = callHttp(requestId, url, http_method, http_rest,
                header, body, http_ok_field, http_ok_value, i.timeout)

            map ++= res
            map.put(codeTag, code)
        } else if (remote == "") {
            val (serviceId, msgId) = Flow.router.serviceNameToId(i.service)
            if (serviceId == 0 || msgId == 0) {
                val tp = (params, HashMapStringAny("$code" -> (-10242405)))
                saveInvokeToContext(i.id, tp, context)
                return tp
            }

            val (newbody, ec) = Flow.router.encodeRequest(serviceId, msgId, params)
            req_body = newbody
            if (ec != 0) {
                val tp = (params, HashMapStringAny("$code" -> (-10242400)))
                saveInvokeToContext(i.id, tp, context)
                return tp
            }

            val xhead = HashMapStringAny()
            for ((k, v) <- params if k.startsWith("xhead.")) {
                xhead.put(k.substring(6), v)
            }

            val requestId = "TEST" + RequestIdGenerator.nextId()
            val map = HashMapStringAny()
            res_body = map
            var connId = "127.0.0.1:1000:1000"
            if (params.ns("connId") != "") connId = params.ns("connId")
            else if (context.ns("$connId") != "") connId = context.ns("$connId")

            lock.lock();
            try {
                val req = new Request(
                    requestId,
                    connId,
                    generateSequence(),
                    1,
                    serviceId,
                    msgId,
                    xhead,
                    newbody,
                    TestActor)

                ir = null
                ir = Router.main.send(req)

                if (ir == null) {
                    replied.await(i.timeout, TimeUnit.MILLISECONDS)
                }

                if (ir != null) {
                    map ++= ir.res
                    map.put("$code", ir.code)
                } else {
                    map.put("$code", -10242504)
                }

                newbody.put("$requestId", requestId)

            } finally {
                lock.unlock();
            }

        } else {
            val map = HashMapStringAny()
            req_body = params
            res_body = map
            lock.lock();
            try {
                val (serviceId, msgId) = codecs.serviceNameToId(i.service)
                if (serviceId == 0 || msgId == 0) {
                    val tp = (params, HashMapStringAny("$code" -> (-10242405)))
                    saveInvokeToContext(i.id, tp, context)
                    return tp
                }

                val requestId = generateSequence().toString
                req_body.put("$requestId", requestId)

                val xhead = HashMapStringAny()
                for ((k, v) <- params if k.startsWith("xhead.")) {
                    xhead.put(k.substring(6), v)
                }

                val req = new SocRequest(requestId, serviceId, msgId, req_body, AvenueCodec.ENCODING_UTF8, xhead)
                remoteReqRes = null
                soc.send(req, timeout)

                if (remoteReqRes == null) {
                    replied.await(i.timeout, TimeUnit.MILLISECONDS)
                }

                if (remoteReqRes != null) {
                    map ++= remoteReqRes.res.body
                    map.put("$code", remoteReqRes.res.code)
                } else {
                    map.put("$code", -10242504)
                }

            } finally {
                lock.unlock();
            }
        }
        val tp = (req_body, res_body)
        saveInvokeToContext(i.id, tp, context)
        return tp
    }

    def hasUploadField(body: HashMapStringAny): Boolean = {
        for ((k, v) <- body if v != null) {
            if (v.toString.startsWith("upload_file:")) return true
        }
        false
    }

    def callHttp(requestId: String, url: String, http_method: String, http_rest: String,
                 headers: HashMapStringString, body: HashMapStringAny,
                 http_ok_field: String, http_ok_value: String, timeout: Int): Tuple2[String, HashMapStringAny] = {

        val (ssl, host, path) = parseHostPath(url)
        var bodyStr = ""
        var contentType = ""
        var bodyData: ChannelBuffer = null
        var download_field: Tuple2[String, String] = null

        var real_path = path
        if (http_rest == "true") {
            contentType = "application/json"
            bodyStr = genJsonData(body)
        } else {
            if (hasUploadField(body)) {

                val boundary = "----WebKitFormBoundarywzMkGqJZUrg4hAxZ"
                contentType = "multipart/form-data; boundary=" + boundary
                bodyStr = genFormData(body)
                if (real_path.indexOf("?") < 0)
                    real_path += "?"
                real_path += "&" + bodyStr
                bodyStr = ""
                bodyData = genFormDataForUpload(boundary, body)

            } else {
                contentType = "application/x-www-form-urlencoded"
                bodyStr = genFormData(body)
                download_field = getDownloadField(body)

                if (http_method == "get") {
                    if (real_path.indexOf("?") < 0)
                        real_path += "?"
                    real_path += "&" + bodyStr
                    bodyStr = ""
                }
            }
        }

        val (code, res, cookies) = httpClient.sendWithReturn(http_method, timeout, ssl, host, real_path,
            bodyStr, bodyData, download_field, headers, contentType, g_cookies)
        val map = JsonCodec.parseObjectNotNull(res)
        if (code != 0) return (code.toString, map)
        if (download_field != null) {
            if (map.contains(download_field._1)) {
                map.put(http_ok_field, http_ok_value)
            } else {
                map.put(http_ok_field, "-10242404")
            }
        }
        val v = map.ns(http_ok_field)
        if (v == http_ok_value) {
            if (cookies != "") g_cookies.put(host, cookies)
            return ("0", map)
        } else {
            return (v, map)
        }
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

    def genJsonData(body: HashMapStringAny): String = {
        if (body.contains("body")) return body.ns("body")
        val newmap = HashMapStringAny()
        newmap ++= body
        newmap.remove("$requestId")
        JsonCodec.mkString(newmap)
    }

    def genFormDataForUpload(boundary: String, body: HashMapStringAny): ChannelBuffer = {
        val b = ChannelBuffers.dynamicBuffer()

        val splitter = ""
        val nl = "\r\n".getBytes()

        for ((key, value) <- body if value != null && (value.toString.startsWith("upload_value:") || value.toString.startsWith("upload_file:"))) {
            if (value.toString.startsWith("upload_value:")) {
                val vs = value.toString
                val v = vs.substring(vs.indexOf(":") + 1)
                b.writeBytes(boundary.getBytes())
                b.writeBytes(nl)
                val k = """Content-Disposition: form-data; name="%s"""".format(key)
                b.writeBytes(k.getBytes())
                b.writeBytes(nl)
                b.writeBytes(nl)
                b.writeBytes(v.getBytes("utf-8"))
                b.writeBytes(nl)
            }
            if (value.toString.startsWith("upload_file:")) {
                val vs = value.toString
                val f = vs.substring(vs.indexOf(":") + 1)

                b.writeBytes(boundary.getBytes())
                b.writeBytes(nl)
                val k = """Content-Disposition: form-data; name="%s"; filename="%s"""".format(key, f)
                b.writeBytes(k.getBytes())
                b.writeBytes(nl)
                b.writeBytes("Content-Type: application/octet-stream".getBytes())
                b.writeBytes(nl)
                b.writeBytes(nl)
                val len = new File(f).length.toInt
                val in = new FileInputStream(f)
                b.writeBytes(in, len)
                in.close()
                b.writeBytes(nl)
            }
        }

        b.writeBytes(boundary.getBytes())
        b.writeBytes("--".getBytes())
        b
    }

    def getDownloadField(body: HashMapStringAny): Tuple2[String, String] = {
        for ((key, value) <- body if value != null && value.toString.startsWith("download_file:")) {
            val vs = value.toString
            val f = vs.substring(vs.indexOf(":") + 1)
            return (key, f)
        }
        null
    }

    def genFormData(body: HashMapStringAny): String = {
        val bodyStr = new StringBuilder()
        var first = true

        for ((key, value) <- body if key != "$requestId" && !value.toString.startsWith("download_file:") && !value.toString.startsWith("upload_value:") && !value.toString.startsWith("upload_file:")) {
            if (!first) bodyStr.append("&")
            bodyStr.append(key + "=" + URLEncoder.encode(value.toString, "utf-8"))
            first = false
        }

        bodyStr.toString
    }

    def saveGlobalMock() {
        if (remote != "") return

        savedMocks.clear()
        for ((k, buff) <- Router.main.mocks) {
            val newbuff = ArrayBuffer[MockCfg]()
            newbuff ++= buff
            savedMocks.put(k, newbuff)
        }
    }

    // 目前仅支持最简单的常量，不允许变量，不支持作用域, 相同名字的后定义的会覆盖前面的定义
    def installDefines(d: TestCaseV2Define, context: HashMapStringAny) {
        for ((k, v) <- d.defines) {
            context.put(k, ValueParser.parseRightValue(v, null, context))
        }
    }

    def installMock(m: TestCaseV2Invoke): Tuple2[Boolean, String] = {
        if (remote != "") return (false, "mock cannot be installed in remote mode")

        val service = m.service.toLowerCase
        val (serviceId, msgId) = Flow.router.serviceNameToId(service)
        if (serviceId == 0 || msgId == 0) {
            return (false, "mock failed, service not found")
        }
        val req = HashMapStringAny()
        val res = HashMapStringAny()

        req ++= m.req
        res ++= m.res

        val key = serviceId + ":" + msgId
        var buff = Router.main.mocks.getOrElse(key, null)
        if (buff == null) {
            buff = ArrayBuffer[MockCfg]()
            Router.main.mocks.put(key, buff)
        }
        buff += new MockCfg(key, req, res)
        (true, "success")
    }

    def dump(global: TestCaseV2, testcases: ArrayBuffer[TestCaseV2]) {
        if (global != null) {
            println(global.toString())
            println()
        }
        for (t <- testcases) {
            println(t.toString())
            println()
        }
    }

    def parseFile(lines: ArrayBufferString): Tuple2[TestCaseV2, ArrayBuffer[TestCaseV2]] = {
        var i = 0

        var global: TestCaseV2 = null
        var testcases = ArrayBuffer[TestCaseV2]()

        while (i < lines.size) {
            val t = lines(i)
            t match {
                case t if t.startsWith("global:") =>
                    val (l_global, nextLine) = parseTestCase("global", lines, i)
                    if (global != null) {
                        println("global can be defined only once")
                        return null
                    }
                    global = l_global
                    i = nextLine
                case t if t.startsWith("testcase:") =>
                    val (l_testcase, nextLine) = parseTestCase("testcase", lines, i)
                    testcases += l_testcase
                    i = nextLine
                case _ =>
                    println("line not valid: " + t)
                    return null
            }
        }

        (global, testcases)
    }

    def parseTestCase(tp: String, lines: ArrayBufferString, start: Int): Tuple2[TestCaseV2, Int] = {

        testCaseCount += 1
        var name = parseAttr(lines(start), "testcase")
        var enabled = parseAttr(lines(start), "enabled")
        var extendsFrom = parseAttr(lines(start), "extends")
        var lineNo = parseAttr(lines(start), "lineNo")
        var pluginObjectName = parseAttr(lines(start), "pluginObjectName")
        var remote = parseAttr(lines(start), "remote")
        if (remote == "0") remote = ""

        var http_base_url = parseAttr(lines(start), "http_base_url")
        var http_method = parseAttr(lines(start), "http_method")
        var http_rest = parseAttr(lines(start), "http_rest")
        var http_ok_field = parseAttr(lines(start), "http_ok_field")
        var http_ok_value = parseAttr(lines(start), "http_ok_value")

        if (name == "") name = "testcase_" + testCaseCount
        if (tp == "global") name = "global"
        var i = start + 1

        val commands = ArrayBuffer[TestCaseV2Command]()

        var over = false
        while (i < lines.size && !over) {
            val t = lines(i)
            t match {
                case t if t.startsWith("define:") =>
                    commands += parseDefine(lines, i)
                    i += 1
                case t if t.startsWith("mock:") =>
                    commands += parseInvoke("mock", lines, i)
                    i += 1
                case t if t.startsWith("setup:") =>
                    commands += parseInvoke("setup", lines, i)
                    i += 1
                case t if t.startsWith("setup_http:") =>
                    commands += parseInvoke("setup_http", lines, i)
                    i += 1
                case t if t.startsWith("teardown:") =>
                    commands += parseInvoke("teardown", lines, i)
                    i += 1
                case t if t.startsWith("teardown_http:") =>
                    commands += parseInvoke("teardown_http", lines, i)
                    i += 1
                case t if t.startsWith("assert:") =>
                    commands += parseInvoke("assert", lines, i)
                    i += 1
                case t if t.startsWith("assert_http:") =>
                    commands += parseInvoke("assert_http", lines, i)
                    i += 1
                case t if t.startsWith("testcase:") =>
                    over = true
                case t if t.startsWith("global:") =>
                    over = true
                case _ =>
                    println("invalid line in testcase: " + t)
                    i += 1
            }
        }
        val o = new TestCaseV2(tp, name, commands)
        if (lineNo != "") o.lineNo = lineNo.toInt
        o.enabled = enabled.toLowerCase != "0" && enabled.toLowerCase != "false"
        o.extendsFrom = extendsFrom
        o.pluginObjectName = pluginObjectName
        o.remote = remote
        o.http_base_url = http_base_url
        o.http_method = http_method
        o.http_rest = http_rest
        o.http_ok_field = http_ok_field
        o.http_ok_value = http_ok_value
        (o, i)
    }

    def parseDefine(lines: ArrayBufferString, start: Int): TestCaseV2Define = {
        val line = lines(start)
        val p = parseDefine(line)
        val lineNo = parseAttr(line, "lineNo")
        val map = parseMap(p)
        val map2 = new LinkedHashMapStringAny()
        for ((k, v) <- map) {
            if (k.startsWith("$"))
                map2.put(k, v)
            else
                map2.put("$" + k, v)
        }
        val d = new TestCaseV2Define(map2)
        if (lineNo != "") d.lineNo = lineNo.toInt
        d
    }

    def parseInvoke(tp: String, lines: ArrayBufferString, start: Int): TestCaseV2Invoke = {
        val line = lines(start)
        val service = parseAttr(line, tp)
        val lineNo = parseAttr(line, "lineNo")
        var timeout = parseAttr(line, "timeout")
        if (timeout == "") timeout = "15000"
        var id = parseAttr(line, "id")
        if (id != "" && !id.startsWith("$")) id = "$" + id
        val req = parseReq(line)
        val res = parseRes(line)
        val reqMap = parseMap(req)
        val resMap = parseMap(res)
        if (tp != "mock" && !resMap.contains(codeTag)) resMap.put(codeTag, 0)
        val t = new TestCaseV2Invoke(tp, service, timeout.toInt, reqMap, resMap, id)
        if (lineNo != "") t.lineNo = lineNo.toInt
        if (tp.endsWith("_http")) {
            t.http_method = parseAttr(line, "http_method")
            t.http_rest = parseAttr(line, "http_rest")
            t.http_ok_field = parseAttr(line, "http_ok_field")
            t.http_ok_value = parseAttr(line, "http_ok_value")
        }
        t
    }

    val sep1 = (ValueParser.sepMax + 1).toString // blank
    val sep2 = (ValueParser.sepMax + 2).toString // =

    val r1 = """ ([^ =]+)=""".r

    def parseMap(s: String): LinkedHashMapStringAny = {
        val map = new LinkedHashMapStringAny()

        var ns = escape(s)
        ns = r1.replaceAllIn(" " + ns, (m) => sep1 + m.group(1).replace("$", "\\$") + sep2) // $0 ... $9 有特殊含义，$需转义
        val ss = ns.split(sep1).map(_.trim)
        for (t <- ss) { // if t.indexOf(sep2) > 0
            val tt = t.split(sep2)
            var key = parseKey(tt(0))
            var op = "="
            if (key.endsWith("!")) {
                key = key.substring(0, key.length - 1)
                op = "!="
            }
            if (tt.size >= 2) map.put(key, parseValue(tt(1)))
            else if (tt.size >= 1 && key != "") map.put(key, "")

            if (op != "=") map.put(key + ValueParser.OP_PREFIX, op)
        }
        map
    }

    def escape(s: String): String = {
        ValueParser.escape0(s)
    }

    def parseKey(s: String): String = {
        val t = ValueParser.unescape(s.trim())
        t
    }

    def parseValue(s: String): Any = {
        val t = ValueParser.unescape(s.trim())
        t
    }

    def parseDefine(l: String): String = {
        val p1 = l.indexOf("define:")
        if (p1 < 0) return ""
        l.substring(p1 + 7)
    }
    def parseReq(l: String): String = {
        val p1 = l.indexOf(" req:")
        if (p1 < 0) return ""
        val p2 = l.indexOf(" res:", p1 + 1)
        if (p2 < 0) return l.substring(p1 + 5)
        l.substring(p1 + 5, p2)
    }
    def parseRes(l: String): String = {
        val p1 = l.indexOf(" res:")
        if (p1 < 0) return ""
        l.substring(p1 + 5)
    }
    def parseAttr(s: String, field: String): String = {
        var l = s
        var p = l.indexOf("res:")
        if (p >= 0) l = l.substring(0, p)
        p = l.indexOf("req:")
        if (p >= 0) l = l.substring(0, p)
        val p1 = l.indexOf(field + ":")
        if (p1 < 0) return ""
        val p2 = l.indexOf(" ", p1 + 1)
        if (p2 < 0) return l.substring(p1 + field.length + 1)
        l.substring(p1 + field.length + 1, p2)
    }

    def appendLineNo(line: String): String = {
        lineNoCnt += 1
        line.trim match {
            case l if l.startsWith("mock:") =>
                appendLineNo(line, lineNoCnt)
            case l if l.startsWith("setup:") =>
                appendLineNo(line, lineNoCnt)
            case l if l.startsWith("setup_http:") =>
                appendLineNo(line, lineNoCnt)
            case l if l.startsWith("teardown:") =>
                appendLineNo(line, lineNoCnt)
            case l if l.startsWith("teardown_http:") =>
                appendLineNo(line, lineNoCnt)
            case l if l.startsWith("assert:") =>
                appendLineNo(line, lineNoCnt)
            case l if l.startsWith("assert_http:") =>
                appendLineNo(line, lineNoCnt)
            case _ =>
                line
        }

    }

    def appendLineNo(line: String, n: Int): String = {
        val s = " lineNo:" + n + " "
        if (line.indexOf(" ") > 0)
            line.replaceFirst(" ", s)
        else
            line + s
    }

    def mergeLines(lines: Buffer[String]): ArrayBufferString = {
        val buff = ArrayBufferString()
        for (i <- 0 until lines.size) {
            val t = lines(i)
            t.trim match {
                case l if l.startsWith("define:") =>
                    buff += l
                case l if l.startsWith("global:") =>
                    buff += l
                case l if l.startsWith("testcase:") =>
                    buff += l
                case l if l.startsWith("mock:") =>
                    buff += l
                case l if l.startsWith("setup:") =>
                    buff += l
                case l if l.startsWith("setup_http:") =>
                    buff += l
                case l if l.startsWith("teardown:") =>
                    buff += l
                case l if l.startsWith("teardown_http:") =>
                    buff += l
                case l if l.startsWith("assert:") =>
                    buff += l
                case l if l.startsWith("assert_http:") =>
                    buff += l
                case l if l.startsWith("#") =>
                    buff += l
                case _ =>
                    buff(buff.size - 1) = buff(buff.size - 1) + " " + t
            }
        }
        buff
    }

    def removeComment(line: String): String = {
        val p = line.lastIndexOf(" #")
        if (p >= 0) return line.substring(0, p)
        line
    }

    def isNewFormat(file: String): Boolean = {
        Source.fromFile(file, "UTF-8").getLines.map(_.trim).filter(l => l.startsWith("testcase:") || l.startsWith("global:")).size > 0
    }

}

class TestCaseV1(val serviceId: Int, val msgId: Int, val body: HashMapStringAny, val repeat: Int = 1, val xhead: HashMapStringAny = new HashMapStringAny())

object TestCaseRunnerV1 extends Logging {

    var requestCount = 1
    var replyCount = 0
    var soc: SocImpl = _

    val lock = new ReentrantLock(false)
    val replied = lock.newCondition()

    def initProfile(rootDir: String) {
        val profile = System.getProperty("scalabpe.profile")
        if (profile != null && profile != "") {
            Router.profile = profile
            Router.parameterXml = "parameter_" + profile + ".xml"
            val filename = "config_" + profile + ".xml"
            if (new File(rootDir + File.separator + filename).exists) {
                Router.configXml = filename
            }
        }
        log.info("current profile=" + Router.profile)
        log.info("use config file=" + Router.configXml)
        log.info("use config paramter file=" + Router.parameterXml)
    }

    def loadTestServerAddr(): String = {
        val rootDir = "."

        initProfile(rootDir)

        val in = new InputStreamReader(new FileInputStream("." + File.separator + Router.configXml), "UTF-8")
        val cfgXml = XML.load(in)
        val port = (cfgXml \ "SapPort").text.toInt
        var serverAddr = "127.0.0.1:" + port
        var s = (cfgXml \ "TestServerAddr").text
        if (s != "") serverAddr = s
        serverAddr
    }

    def main(args: Array[String]) {

        var max = 2000000
        var s = parseArg(args, "maxPackageSize")
        if (s != "") max = s.toInt

        var file = "." + File.separator + "testcase" + File.separator + "default.txt"
        s = parseFile(args)
        if (s != "") file = s
        println()
        println("testcase file:  " + file)

        val codecs = new TlvCodecs("." + File.separator + "avenue_conf")

        val serverAddr = loadTestServerAddr()
        val timeout = 15000
        soc = new SocImpl(serverAddr, codecs, callback, connSizePerAddr = 1, maxPackageSize = max)

        val lines = Source.fromFile(file, "UTF-8").getLines.toList.filter(_.trim != "").filter(!_.startsWith("#"))

        val testcases = parseLines(lines)

        //val sendCount = testcases.foldLeft(0) { (sum,tc) => sum + tc.repeat }

        println("total test case: " + testcases.size)

        var seq = 0

        for (i <- 1 to testcases.size) {

            val t = testcases(i - 1)

            lock.lock();
            try {
                for (j <- 1 to t.repeat) {
                    seq += 1

                    replyCount = 0

                    val req = new SocRequest(seq.toString, t.serviceId, t.msgId, t.body, AvenueCodec.ENCODING_UTF8, t.xhead)
                    println("req=" + req)
                    soc.send(req, timeout)
                    if (replyCount < requestCount)
                        replied.await(timeout + 100, TimeUnit.MILLISECONDS)

                }
            } finally {
                lock.unlock();
            }

        }

        soc.close
    }

    def callback(any: Any) {

        any match {

            case reqRes: SocRequestResponseInfo =>

                lock.lock();
                try {
                    val reqRes = any.asInstanceOf[SocRequestResponseInfo]
                    println("res=" + reqRes.res)
                    replyCount += 1
                    if (replyCount >= requestCount)
                        replied.signal()
                } finally {
                    lock.unlock();
                }

            case ackInfo: SocRequestAckInfo =>

                println("ack=" + ackInfo.req.requestId)

            case _ =>
        }
    }
    def parseLines(lines: List[String]): ArrayBuffer[TestCaseV1] = {

        val testcases = new ArrayBuffer[TestCaseV1]()

        for (line <- lines) {
            val tc = parseLine(line)
            if (tc != null)
                testcases += tc
        }

        testcases
    }

    def parseArg(args: Array[String], key: String): String = {
        for (i <- 0 until args.size) {
            if (args(i) == "-" + key) {
                return "1"
            }
        }
        ""
    }

    def parseFile(args: Array[String]): String = {
        var i = 0
        while (i < args.size) {
            if (args(i).startsWith("-")) i += 1
            else return args(i)
        }
        ""
    }

    def parseLine(line: String): TestCaseV1 = {

        val p1 = line.indexOf(",")
        if (p1 <= 0) return null

        val p2 = line.indexOf(",", p1 + 1)
        if (p2 <= 0) return null

        val serviceId = line.substring(0, p1).toInt
        val msgId = line.substring(p1 + 1, p2).toInt
        val json = line.substring(p2 + 1)

        val body = JsonCodec.parseObject(json)

        var repeat = body.i("x_repeat")
        if (repeat == 0) repeat = 1

        val xhead = HashMapStringAny()
        val socId = body.s("x_socId", "")
        if (socId != "") xhead.put("socId", socId)
        val spsId = body.s("x_spsId", "")
        if (spsId != "") xhead.put("spsId", spsId)
        val uniqueId = body.s("x_uniqueId", "")
        if (uniqueId != "") xhead.put("uniqueId", uniqueId)
        val appId = body.s("x_appId", "")
        if (appId != "") xhead.put("appId", appId)

        val tc = new TestCaseV1(serviceId, msgId, body, repeat, xhead)
        tc
    }

}

class RunTestHttpClient(
        val connectTimeout: Int = 15000,
        val timerInterval: Int = 1000) extends HttpClient4Netty with Logging {

    var nettyHttpClient: NettyHttpClient = _
    val generator = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[Int, CacheData]()
    val localIp = IpUtils.localIp()

    init

    def init() {
        nettyHttpClient = new NettyHttpClient(this, connectTimeout, timerInterval, 5000000)
        log.info("RunTestHttpClient started")
    }

    def close() {
        nettyHttpClient.close()
        log.info("RunTestHttpClient stopped")
    }

    def sendWithReturn(method: String, timeout: Int, ssl: Boolean, host: String, path: String, body: String, bodyData: ChannelBuffer, download_field: Tuple2[String, String],
                       headers: HashMapStringString = null, contentType: String = "application/x-www-form-urlencoded",
                       cookies: HashMapStringString = HashMapStringString()): Tuple3[Int, String, String] = {
        val lock = new ReentrantLock();
        val cond = lock.newCondition();
        val r = new AtomicReference[Tuple3[Int, String, String]]();
        val callback = (res: Tuple3[Int, String, String]) => {
            lock.lock();
            try {
                r.set(res);
                cond.signal();
            } finally {
                lock.unlock();
            }
        }

        lock.lock();
        try {
            send(callback, method, timeout, ssl, host, path, body, bodyData, download_field, headers, contentType, cookies)
            try {
                val ok = cond.await(timeout, TimeUnit.MILLISECONDS);
                if (ok) {
                    return r.get();
                }
                return (ResultCodes.SERVICE_TIMEOUT, "", "")
            } catch {
                case e: InterruptedException =>
                    return (ResultCodes.SOC_NETWORKERROR, "", "")
            }
        } finally {
            lock.unlock();
        }

    }

    def send(callback: (Tuple3[Int, String, String]) => Unit, method: String, timeout: Int, ssl: Boolean,
             host: String, path: String, body: String, bodyData: ChannelBuffer, download_field: Tuple2[String, String],
             headers: HashMapStringString = null, contentType: String = "application/x-www-form-urlencoded", cookies: HashMapStringString = HashMapStringString()): Unit = {
        var httpReq = generateRequest(host, path, method, body, bodyData, headers, contentType, cookies)
        val sequence = generateSequence()
        dataMap.put(sequence, new CacheData(download_field, callback))

        nettyHttpClient.send(sequence, ssl, host, httpReq, timeout)
    }

    def receive(sequence: Int, httpRes: HttpResponse): Unit = {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val tpl = parseResult(saved.download_field, httpRes)
        saved.callback(tpl)
    }

    def networkError(sequence: Int) {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val tpl = (ResultCodes.SOC_NETWORKERROR, "", "")
        saved.callback(tpl)
    }

    def timeoutError(sequence: Int) {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val tpl = (ResultCodes.SOC_TIMEOUT, "", "")
        saved.callback(tpl)
    }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def generateRequest(host: String, path: String, method: String, body: String, bodyData: ChannelBuffer,
                        headers: HashMapStringString = null, contentType: String = "application/x-www-form-urlencoded", cookies: HashMapStringString = HashMapStringString()): HttpRequest = {

        val m = method.toLowerCase match {
            case "get"    => HttpMethod.GET
            case "put"    => HttpMethod.PUT
            case "post"   => HttpMethod.POST
            case "delete" => HttpMethod.DELETE
            case _        => HttpMethod.GET
        }
        val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, m, path)
        httpReq.setHeader("Host", host)
        httpReq.setHeader("User-Agent", "scalabpe runtest client")
        httpReq.setHeader("Connection", "close")

        if (headers != null) {
            for ((k, v) <- headers) {
                httpReq.setHeader(k, v)
            }
        }

        val cookiesMap = JsonCodec.parseObjectNotNull(cookies.getOrElse(host, ""))
        if (cookiesMap.size > 0) {
            val encoder = new CookieEncoder(false);
            for ((k, v) <- cookiesMap) {
                encoder.addCookie(k, v.toString)
            }
            httpReq.setHeader("Cookie", encoder.encode());
        }

        if (body.length > 0) {
            if (log.isDebugEnabled()) {
                log.debug("method=" + method + ", path=" + path + ", content=" + body)
            }

            val buffer = new DynamicChannelBuffer(512);
            buffer.writeBytes(body.getBytes("UTF-8"))
            httpReq.setContent(buffer);
            httpReq.setHeader("Content-Type", contentType)
            httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex)
        }
        if (bodyData != null) {
            if (log.isDebugEnabled()) {
                log.debug("method=" + method + ", path=" + path + ", file upload")
            }

            httpReq.setContent(bodyData);
            httpReq.setHeader("Content-Type", contentType)
            httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex)
        }

        httpReq
    }

    def parseResult(download_field: Tuple2[String, String], httpRes: HttpResponse): Tuple3[Int, String, String] = {

        val status = httpRes.getStatus
        if (status.getCode() != 200 && status.getCode() != 201 && status.getCode() != 302) {

            if (log.isDebugEnabled()) {
                log.debug("status code={}", status.getCode())
            }

            return (ResultCodes.SOC_NETWORKERROR, "", "")
        }

        val contentTypeStr = httpRes.getHeader("Content-Type")
        val content = httpRes.getContent()
        var contentStr = ""
        if (download_field == null) {
            if (content != null)
                contentStr = content.toString(Charset.forName("UTF-8"))
        } else {
            try {
                val out = new FileOutputStream(download_field._2)
                val buff = content.toByteBuffer()
                out.write(buff.array(), 0, buff.limit())
                out.close()
                contentStr = """{"%s":"%s"}""".format(download_field._1, download_field._2)
            } catch {
                case e: Throwable =>
                    log.error("cannot save to file " + download_field._2)
                    return (ResultCodes.SOC_NETWORKERROR, "", "")
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("contentType={},contentStr={}", contentTypeStr, contentStr)
        }

        val cookie = httpRes.getHeader("Set-Cookie");
        var cookie_json = ""
        val cookieMap = HashMapStringAny()
        if (cookie != null && cookie != "") {
            val cookies = new CookieDecoder().decode(cookie);
            val i = cookies.iterator()
            while (i.hasNext()) {
                val c = i.next()
                cookieMap.put(c.getName(), c.getValue())
            }
            if (cookieMap.size > 0) {
                cookie_json = JsonCodec.mkString(cookieMap)
            }
        }
        if (status.getCode() == 302 && contentStr == "") {
            contentStr = """{"code":0,"Location":"%s"}""".format(httpRes.getHeader("Location"))
        }
        (0, contentStr, cookie_json)
    }

    class CacheData(val download_field: Tuple2[String, String], val callback: (Tuple3[Int, String, String]) => Unit)

}
