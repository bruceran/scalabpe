package scalabpe.plugin.http

import java.io.File
import java.io.FileInputStream
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.GregorianCalendar
import java.util.Locale
import java.util.TimeZone

import scala.collection.mutable.HashMap

import org.jboss.netty.handler.codec.http.HttpHeaders

import scalabpe.core.HashMapStringAny

class DownloadPlugin extends HttpServerPlugin with HttpServerRawOutputPlugin {

    val ETAG_TAG = "etag"
    val EXPIRE_TAG = "expire"
    val LASTMODIFIED_TAG = "lastModified"
    val DELETE_TAG = "delete"
    val ATTACHMENT = "attachment"
    val FILENAME = "filename"
    val CONTENT = "content"

    val HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    val HTTP_DATE_GMT_TIMEZONE = "GMT";

    val df_tl = new ThreadLocal[SimpleDateFormat]() {
        override def initialValue(): SimpleDateFormat = {
            val df = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US)
            df.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));
            df
        }
    }

    def generateRawContent(serviceId: Int, msgId: Int, errorCode: Int, errorMessage: String, body: HashMapStringAny, pluginParam: String, headers: HashMap[String, String]): Array[Byte] = {

        if (body.ns(ETAG_TAG) != "") {
            headers.put("ETag", body.ns(ETAG_TAG))
        }

        if (body.ns(LASTMODIFIED_TAG) != "") { // seconds
            body.l(LASTMODIFIED_TAG) match {
                case 0 | -1 =>
                    headers.put(HttpHeaders.Names.LAST_MODIFIED, df_tl.get.format(new Date()))
                case n =>
                    headers.put(HttpHeaders.Names.LAST_MODIFIED, df_tl.get.format(new Date(1000 * body.l(LASTMODIFIED_TAG))))
            }
        }

        if (body.ns(EXPIRE_TAG) != "") {
            body.i(EXPIRE_TAG) match {
                case 0 | -1 =>
                    headers.put(HttpHeaders.Names.CACHE_CONTROL, "no-cache")
                case n => // seconds
                    val time = new GregorianCalendar();
                    time.add(Calendar.SECOND, n);
                    headers.put(HttpHeaders.Names.EXPIRES, df_tl.get.format(time.getTime()));
                    headers.put(HttpHeaders.Names.CACHE_CONTROL, "max-age=" + n);
            }
        }

        if (body.ns(FILENAME) != "") {
            val filename = body.ns(FILENAME)
            val ext = parseExt(filename)
            if (ext != "")
                body.put("__file_ext__", ext)
        }

        if (body.ns(FILENAME) != "" && body.ns(ATTACHMENT, "1") == "1") {
            val filename = body.ns(FILENAME)
            val v = "attachment; filename=\"%s\"".format(URLEncoder.encode(parseFilename(filename), "UTF-8"))
            headers.put("Content-Disposition", v)
        }

        if (body.contains(CONTENT)) {
            val buff = body.getOrElse(CONTENT, null).asInstanceOf[Array[Byte]]
            if (buff != null) return buff
        }

        if (body.ns(FILENAME) != "") {

            val file = body.ns(FILENAME)
            if (new File(file).exists()) {
                val buff = readFile(file)
                if (body.ns(DELETE_TAG) == "1") {
                    new File(file).delete()
                }
                if (buff != null) return buff
            }
        }

        null
    }

    def readFile(filename: String): Array[Byte] = {
        val len = new File(filename).length().toInt
        val buffer = new Array[Byte](len)
        val in = new FileInputStream(filename)
        try {
            in.read(buffer)
            return buffer
        } catch {
            case e: Throwable =>
                return null
        } finally {
            in.close()
        }
    }

    def parseFilename(name: String): String = {
        val p = name.lastIndexOf("/")
        if (p < 0) return name
        name.substring(p + 1)
    }
    def parseExt(name: String): String = {
        val p = name.lastIndexOf(".")
        if (p < 0) return ""
        name.substring(p + 1).toLowerCase()
    }

}
