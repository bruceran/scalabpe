package scalabpe.flow

import scalabpe.core._
import scalabpe.plugin.http._
import java.util.TreeMap

class WeiXinPayHttpPlugin extends HttpPlugin with HttpRequestPlugin with Logging {

	def isEmpty(s:String) : Boolean = if( s == null || s == "" ) true else false

	override def generateRequestBody(msg:HttpMsgDefine,body:HashMapStringAny):String = {
		
		log.info("weiXinPayHttpPlugin.generateRequestBody called")

		val sb = new StringBuilder
		val sbSign = new StringBuilder
		sb.append("<xml>")
		var treeMap = new TreeMap[String, String]()
		for ((key, value) <- body) {
			if( !isEmpty(body.s(key)) && (key !="notifyUrl") && (key != "signatureKey") ){
				treeMap.put(key, TypeSafe.anyToString(value, ""))
				sb.append("<").append(key).append(">").append(value).append("</").append(key).append(">").append("\n")
			}
		}
		val entries = treeMap.entrySet().iterator()
		while (entries.hasNext()) {
			val entry = entries.next()
			var key = entry.getKey
			var value = entry.getValue
			if (value != null) {
				sbSign.append("&").append(key).append("=").append(TypeSafe.anyToString(value, ""))
			}
		}
		var signOriginal=""
		if (sbSign.size > 0) {
			signOriginal = sbSign.substring(1) +"&key=" + body.s("signatureKey")
		}
		log.info("weixinPay postData: "+signOriginal)

		var md5 = CryptHelper.md5(signOriginal).toUpperCase()
		sb.append("<sign>").append(md5).append("</sign>")
		sb.append("</xml>")
		
		log.info("weixinPay postData:"+sb.toString)
		
		sb.toString
	}

}
