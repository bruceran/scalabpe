package scalabpe.plugin.http

import java.io.File
import java.io.StringWriter
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine

import scalabpe.core.ArrayBufferInt
import scalabpe.core.ArrayBufferLong
import scalabpe.core.ArrayBufferDouble
import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.Flow
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging;

object TemplateUtils {

    val templateDir = Flow.router.rootDir + File.separator + "webapp" + File.separator + "template"
    val templateCache = Flow.router.getConfig("httpserver.template.cache", "1") == "1"
    val templateTypeMap = new ConcurrentHashMap[String, String]()

    def getExt(f: String): String = {
        val p = f.lastIndexOf(".")
        if (p < 0) return ""
        val ext = f.substring(p + 1).toLowerCase
        ext
    }

    def getTemplateType(templateName: String, ext: String, tp: String): String = {
        var f = new File(templateDir + File.separator + templateName + ext)
        if (f.exists()) return tp
        null
    }
    def getTemplateType(templateName: String): String = {

        var templateType: String = null

        if (templateCache)
            templateType = templateTypeMap.get(templateName)
        if (templateType != null) return templateType

        templateType = getTemplateType(templateName, "", "simple")
        if (templateType == null)
            templateType = getTemplateType(templateName, ".vm", "velocity")
        if (templateType == null)
            templateType = "unknown"

        if (templateCache)
            templateTypeMap.put(templateName, templateType)

        templateType
    }

}

class TemplatePlugin extends HttpServerPlugin with HttpServerOutputPlugin with Logging {

    def generateContent(serviceId: Int, msgId: Int, errorCode: Int, errorMessage: String, body: HashMapStringAny, pluginParam: String): String = {

        var templateName = body.s("templateName", "")
        if (templateName == null || templateName == "") templateName = pluginParam
        if (templateName == null || templateName == "") {
            log.error("template name is empty, service:%d,msgId:%d".format(serviceId, msgId))
            return ""
        }
        val ext = TemplateUtils.getExt(templateName)
        if (ext != "") body.put("__file_ext__", ext)

        val templateType = TemplateUtils.getTemplateType(templateName)

        templateType match {
            case "simple" =>
                SimpleTemplate.generateContent(serviceId, msgId, errorCode, errorMessage, body, templateName)
            case "velocity" =>
                VelocityTemplate.generateContent(serviceId, msgId, errorCode, errorMessage, body, templateName)
            case _ =>
                body.put("__file_ext__", "txt")
                "template_not_found"
        }

    }

}

object SimpleTemplate extends Logging {

    class TemplateItem(val tp: Int, val tpData: String)
    class TemplateData(val items: ArrayBuffer[TemplateItem], val lastModified: Long, var lastChecked: Long)

    val templateDir = Flow.router.rootDir + File.separator + "webapp" + File.separator + "template"
    val templateCache = Flow.router.getConfig("httpserver.template.cache", "1") == "1"
    val checkInterval = Flow.router.getConfig("httpserver.template.checkInterval", "60").toInt * 1000
    val templateMap = new ConcurrentHashMap[String, TemplateData]()

    def parseTemplate(content: String, f: File): TemplateData = {

        val items = ArrayBuffer[TemplateItem]()
        var lastp = 0
        var p = content.indexOf("${", lastp)
        while (p >= 0) {
            val t = content.indexOf("}", p)
            if (t >= 0) {
                items += new TemplateItem(1, content.substring(lastp, p))
                items += new TemplateItem(2, content.substring(p + 2, t).trim)
                lastp = t + 1
                p = content.indexOf("${", lastp)
            } else {
                items += new TemplateItem(1, content.substring(lastp))
                lastp = content.length
                p = -1
            }
        }
        if (lastp < content.length)
            items += new TemplateItem(1, content.substring(lastp))
        new TemplateData(items, f.lastModified, System.currentTimeMillis)
    }

    def generateContent(serviceId: Int, msgId: Int, errorCode: Int, errorMessage: String, body: HashMapStringAny, templateName: String): String = {
        val file = templateDir + File.separator + templateName
        var data: TemplateData = null
        if (templateCache) data = templateMap.get(templateName)
        if (data != null) {
            val now = System.currentTimeMillis()
            if (now - data.lastChecked >= checkInterval) {
                if (new File(file).lastModified != data.lastModified) {
                    templateMap.remove(templateName)
                    data = null
                } else {
                    data.lastChecked = now
                }
            }
        }

        if (data == null) {
            val f = new File(file)
            val content = FileUtils.readFileToString(f, "UTF-8")
            data = parseTemplate(content, f)
            if (templateCache)
                templateMap.put(templateName, data)
        }

        if (data == null) {
            log.error("template not found name:%s,service:%d,msgId:%d".format(templateName, serviceId, msgId))
            return "template_not_found"
        }

        // support only single field or field in a map

        val buff = new StringBuilder()
        for (item <- data.items) {
            item.tp match {
                case 1 =>
                    buff.append(item.tpData)
                case 2 =>
                    val value = item.tpData match {
                        case "return_code"    => errorCode.toString
                        case "return_message" => if (errorMessage == null) "" else errorMessage
                        case _ =>
                            val p = item.tpData.indexOf(".")
                            if (p > 0) {
                                val ss = item.tpData.split("\\.")
                                if (ss.length == 2) {
                                    val m = body.m(ss(0))
                                    if (m == null)
                                        ""
                                    else
                                        m.s(ss(1), "")
                                } else {
                                    body.s(item.tpData, "")
                                }
                            } else {
                                body.s(item.tpData, "")
                            }
                    }
                    buff.append(value)
            }
        }

        buff.toString
    }

}

object VelocityTemplate extends Logging {

    val templateDir = Flow.router.rootDir + File.separator + "webapp" + File.separator + "template"
    val templateCache = Flow.router.getConfig("httpserver.template.cache", "1") == "1"
    val checkInterval = Flow.router.getConfig("httpserver.template.checkInterval", "60").toInt

    val ve = new VelocityEngine();
    init()

    def init() {
        ve.setProperty("runtime.log", "velocity.log");
        ve.setProperty("input.encoding", "UTF-8")
        ve.setProperty("output.encoding", "UTF-8")
        ve.setProperty("resource.loader", "file");
        ve.setProperty("file.resource.loader.path", templateDir)
        ve.setProperty("file.resource.loader.cache", if (templateCache) "true" else "false")
        ve.setProperty("file.resource.loader.modificationCheckInterval", checkInterval.toString)
        ve.init()
    }

    def generateContent(serviceId: Int, msgId: Int, errorCode: Int, errorMessage: String, body: HashMapStringAny, templateName: String): String = {

        val t = ve.getTemplate(templateName + ".vm");

        val context = new VelocityContext();
        context.put("return_code", errorCode);
        context.put("return_message", errorMessage);
        for ((k, v) <- body) {
            if (v == null) {
                context.put(k, "")
            } else {
                v match {
                    case s: String => context.put(k, s)
                    case i: Int    => context.put(k, i.toString)
                    case ls: ArrayBufferString =>
                        val l = new java.util.ArrayList[String]
                        for (s <- ls) {
                            l.add(s)
                        }
                        context.put(k, l)
                    case li: ArrayBufferInt =>
                        val l = new java.util.ArrayList[String]
                        for (i <- li) {
                            l.add(i.toString)
                        }
                        context.put(k, l)
                    case ll: ArrayBufferLong =>
                        val l = new java.util.ArrayList[String]
                        for (i <- ll) {
                            l.add(i.toString)
                        }
                        context.put(k, l)
                    case ld: ArrayBufferDouble =>
                        val l = new java.util.ArrayList[String]
                        for (i <- ld) {
                            l.add(i.toString)
                        }
                        context.put(k, l)
                    case m: HashMapStringAny =>
                        val m2 = new java.util.HashMap[String, String]
                        for ((k2, v2) <- m) {
                            m2.put(k2, if (v2 == null) "" else v2.toString)
                        }
                        context.put(k, m2)
                    case lm: ArrayBufferMap =>
                        val l = new java.util.ArrayList[java.util.HashMap[String, String]]
                        for (m <- lm) {
                            val m2 = new java.util.HashMap[String, String]
                            for ((k2, v2) <- m) {
                                m2.put(k2, if (v2 == null) "" else v2.toString)
                            }
                            l.add(m2)
                        }
                        context.put(k, l)

                    case _ => context.put(k, v.toString)
                }
            }
        }

        val writer = new StringWriter();
        t.merge(context, writer);
        writer.toString
    }

}

