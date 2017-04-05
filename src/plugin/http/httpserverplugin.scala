package scalabpe.plugin.http

import org.jboss.netty.handler.codec.http._;
import scalabpe.core._
import scala.collection.mutable.HashMap

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

trait HttpServerRawOutputPlugin {
    def generateRawContent(serviceId:Int,msgId:Int,errorCode:Int,errorMessage:String,body:HashMapStringAny,pluginParam:String,headers:HashMap[String,String]):Array[Byte]
}

trait HttpServerVerifyPlugin {
    def verify(serviceId:Int,msgId:Int,xhead:HashMapStringAny,body:HashMapStringAny,httpReq:HttpRequest):Boolean
}



