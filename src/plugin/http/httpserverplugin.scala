package jvmdbbroker.plugin.http

import org.jboss.netty.handler.codec.http._;
import jvmdbbroker.core._

trait HttpServerPlugin {}

trait HttpServerRequestParsePlugin {
    def parseContent(serviceId:Int,msgId:Int,charset:String,contentType:String,contentStr:String,body:HashMapStringAny ):Unit
}

trait HttpServerRequestPostParsePlugin {
    def afterParse(serviceId:Int,msgId:Int,xhead:HashMapStringAny, body:HashMapStringAny ):Unit
}

trait HttpServerPreOutputPlugin {
    def adjustBody(serviceId:Int,msgId:Int,errorCode:Int,body:HashMapStringAny):HashMapStringAny
}

trait HttpServerOutputPlugin {
    def generateContent(serviceId:Int,msgId:Int,errorCode:Int,errorMessage:String,body:HashMapStringAny,pluginParam:String):String
}

trait HttpServerVerifyPlugin {
    def verify(serviceId:Int,msgId:Int,xhead:HashMapStringAny,body:HashMapStringAny,httpReq:HttpRequest):Boolean
}



