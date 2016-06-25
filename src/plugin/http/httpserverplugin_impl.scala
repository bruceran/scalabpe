package jvmdbbroker.plugin.http

import java.io.File
import scala.collection.mutable.{ArrayBuffer,HashMap}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.jboss.netty.handler.codec.http._;
import jvmdbbroker.core._

class PlainTextPlugin extends HttpServerPlugin with HttpServerOutputPlugin {

    def generateContent(serviceId:Int,msgId:Int,errorCode:Int,errorMessage:String,body:HashMapStringAny,pluginParam:String):String = {
        val fieldName = if( pluginParam == null || pluginParam == "" ) "plainText" else pluginParam
        body.s(fieldName,"")
    }

}

object RedirectPlugin {

val htmlstart = """<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    </head>
    <body>
            <script language="javascript">
                window.location.href=""""

val htmlend = """"
            </script>
    </body>
</html>"""

}

class RedirectPlugin extends HttpServerPlugin with HttpServerOutputPlugin {

    def generateContent(serviceId:Int,msgId:Int,errorCode:Int,errorMessage:String,body:HashMapStringAny,pluginParam:String):String = {
        val fieldName = if( pluginParam == null || pluginParam == "" ) "redirectUrl" else pluginParam
        RedirectPlugin.htmlstart + body.s(fieldName,"") + RedirectPlugin.htmlend
    }

}
