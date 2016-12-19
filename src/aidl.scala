package jvmdbbroker.core

import java.io._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.commons.io.FileUtils
import scala.xml._

/*
    手工编写服务描述文件太麻烦，通过此辅助类可以编写aidl文件再转换成服务描述文件

    aidl文件语法：

    service:xxx 定义服务, 支持的属性：id,auto(若为1则自动生成code或id, 除非指定)
    type:xxx 定义类型, 支持的属性：code,default,array(auto或实际name),desc
    message:xxx 定义消息, 支持的属性：id,uri,desc
    req: 定义请求体
    res: 定义响应体
    #插入空行
    #xxx 插入注释, 1级缩进
    ##xxx 插入注释, 2级缩进
    ###xxx 插入注释, 3级缩进
    其它：struct或message的field,支持的属性：type,default,desc

    desc属性必须是最后一个属性
    type或结构体里的field名称后若不接类型，则默认为string; 否则用 xxx:typename 定义xxx的类型为typename
    所有的type会在名称后自动加上_type
    所有的array type会在名称后自动加上_list_type
    通过为type定义array属性后会自动生成array的type定义
*/

object IdlConverter {

    def main(args:Array[String]) {
        val p = args(0)
        val srcFile = "aidl/" + p + ".aidl"
        val targetFile = "avenue_conf/" + p + ".xml"
        convertFile(srcFile,targetFile)
    }

    val indent = "    "
    val indent2 = indent + indent
    val indent3 = indent + indent + indent

    class Context {
        var lastNode:String = ""
        var auto:Boolean = true
        var arrayName:String = ""
        var itemName:String = ""
    }

    var messageIdSeq = 0
    var typeCodeSeq = 0

    def convertFile(srcFile:String,targetFile:String) {
        val lines = Source.fromFile(srcFile,"UTF-8").getLines.map(_.trim).filter(_ != "").toList
        val buff = ArrayBufferString()
        buff += """<?xml version="1.0" encoding="UTF-8"?>"""
        val ctx = new Context()
        var lineNo = 0
        for( l <- lines ) {
            lineNo += 1
            try {
                convertLine(buff,l,ctx) 
            } catch {
                case e:Throwable =>
                    println(buff.mkString("\n"))
                    println("line#"+lineNo+", "+e.getMessage)
                    return
            }
        }
        endLastNode(buff,ctx)
        buff += ""
        buff += "</service>"

        println(buff.mkString("\n"))

        val s = buff.mkString("\n")
        val f = new File(targetFile)
        FileUtils.writeStringToFile(f,s,"UTF-8")

        try {
            val codec = new TlvCodec(targetFile)
            println("xml is valid")
        } catch {
            case e:Throwable =>
                println("xml is not valid, error="+e.getMessage)
        }
    }

    def endLastNode(buff:ArrayBufferString,ctx:Context) {
        ctx.lastNode match {
            case "struct" =>
                val s = indent + "</type>"
                buff += s
                if( ctx.arrayName != "") {
                    val s = indent + """<type name="%s_type" class="array" itemType="%s_type"/>""".format(ctx.arrayName,ctx.itemName)
                    buff += s 
                }
            case "message" =>
                val s = indent2 + "</responseParameter>\n" + indent + "</message>"
                buff += s 
            case _ =>
        }
        ctx.lastNode = ""
        ctx.arrayName = ""
        ctx.itemName = ""
    }

    def convertLine(buff:ArrayBufferString,s:String,ctx:Context) {

        s match {
            case l if l.startsWith("#") =>
                convertComment(buff,l,ctx)
            case l if l.startsWith("service:") =>
                convertService(buff,l,ctx)
            case l if l.startsWith("type:") =>
                convertType(buff,l,ctx)
            case l if l.startsWith("message:") =>
                convertMessage(buff,l,ctx)
            case l if l.startsWith("req:") =>
                convertReq(buff,l,ctx)
            case l if l.startsWith("res:") =>
                convertRes(buff,l,ctx)
            case _ =>
                convertField(buff,s,ctx)
        }
    }

    def convertComment(buff:ArrayBufferString,l:String,ctx:Context) {
        l match {
            case t if l == "#" =>
                buff += ""
            case t if l.startsWith("###") =>
                val s = indent3 + "<!-- " + l.substring(3) + " -->"
                buff += s 
            case t if l.startsWith("##") =>
                val s = indent2 + "<!-- " + l.substring(2) + " -->"
                buff += s 
            case _ =>
                val s = indent + "<!-- " + l.substring(1) + " -->"
                buff += s 
        }
    }

    def convertService(buff:ArrayBufferString,l:String,ctx:Context) {
        val name = parseAttr(l,"service")
        val id = parseAttr(l,"id")
        val auto = parseAttr(l,"auto")
        ctx.auto = auto == "1"
        val s = """<service name="%s" id="%s" IsTreeStruct="false">""".format(name,id)
        buff += s 
        buff += "" 
    }

    def convertType(buff:ArrayBufferString,l:String,ctx:Context) {
        endLastNode(buff,ctx)
        var name = parseAttr(l,"type")
        val p = name.indexOf(":")
        var cls = "string"
        if( p > 0 ) {
            cls = name.substring(p+1)
            name = name.substring(0,p)
        }
        var code = parseAttr(l,"code")
        if( code == "" ) {
            if( !ctx.auto )
                throw new Exception("line not valid, line:"+l)
            typeCodeSeq += 1
            code = typeCodeSeq.toString
        } else {
            typeCodeSeq = code.toInt
        }
        val desc = parseDesc(l,"desc")
        val default = parseDefault(l,"default")

        var xml = cls match {
            case "string" | "int" =>
                var s = indent + """<type name="%s_type" class="%s" code="%s" default="%s" desc="%s"/>""".format(name,cls,code,default,desc)
                s = s.replace(" desc=\"\"","") 
                s = s.replace(" default=\"NULL\"","") 
                s
            case "struct" =>
                ctx.lastNode = "struct"
                var s = indent + """<type name="%s_type" class="%s" code="%s" desc="%s">""".format(name,cls,code,desc)
                s = s.replace(" desc=\"\"","") 
                s
        }
        val array = parseAttr(l,"array")
        if( array != "" ) {
            if( cls == "struct") {
                val arrayName = if(array == "auto" ) name + "_list" else array
                ctx.arrayName = arrayName
                ctx.itemName = name
            } else {
                val arrayName = if(array == "auto" ) name + "_list" else array
                val s = indent + """<type name="%s_type" class="array" itemType="%s_type"/>""".format(arrayName,name)
                xml =  xml + "\n" + s
            }
        }
        buff += xml
    }

    def convertMessage(buff:ArrayBufferString,l:String,ctx:Context) {
        endLastNode(buff,ctx)

        buff += ""

        val name = parseAttr(l,"message")
        var id = parseAttr(l,"id")
        if( id == "" ) {
            if( !ctx.auto)
                throw new Exception("line not valid, line:"+l)
            messageIdSeq += 1
            id = messageIdSeq.toString
        } else {
            messageIdSeq = id.toInt
        }

        val uri = parseAttr(l,"uri")
        val desc = parseDesc(l,"desc")
        
        var s = indent + """<message name="%s" id="%s" uri="%s" desc="%s">""".format(name,id,uri,desc)
        s = s.replace(" desc=\"\"","") 
        s = s.replace(" uri=\"\"","") 
        buff += s
        ctx.lastNode = "message"
    }

    def convertReq(buff:ArrayBufferString,l:String,ctx:Context) {
        val s = indent2 + "<requestParameter>"
        buff += s 
    }

    def convertRes(buff:ArrayBufferString,l:String,ctx:Context) {
        val s = indent2 + "</requestParameter>\n" + indent2 + "<responseParameter>" 
        buff += s 
    }

    def convertField(buff:ArrayBufferString,l:String,ctx:Context) {
        if(l == "" )
            endLastNode(buff,ctx)

        var name = parseFieldName(l)
        val desc = parseAttr(l,"desc")
        val default = parseDefault(l,"default")
        ctx.lastNode match {
            case "message" =>
                var tp = parseAttr(l,"type")
                if( tp == "" ) tp = name + "_type"
                var xml = indent3 + """<field name="%s" type="%s" default="%s" desc="%s"/>""".format(name,tp,default,desc)
                xml = xml.replace(" desc=\"\"","") 
                xml = xml.replace(" default=\"NULL\"","") 
                buff += xml
            case "struct" =>
                var cls = "systemstring"
                val p = name.indexOf(":")
                if( p > 0 ) {
                    cls = name.substring(p+1)
                    name = name.substring(0,p)
                }
                var xml = indent2 + """<field name="%s" type="%s" default="%s" desc="%s"/>""".format(name,cls,default,desc)
                xml = xml.replace(" desc=\"\"","") 
                xml = xml.replace(" default=\"NULL\"","") 
                buff += xml
            case "" =>
                throw new Exception("line not valid, line:"+l)
        }
    }

    def parseAttr(l:String,field:String):String = {
        val p1 = l.indexOf(field+":")
        if( p1 < 0 ) return ""
        val p2 = l.indexOf(" ",p1+1)
        if( p2 < 0 ) return l.substring(p1+field.length+1)
        l.substring(p1+field.length+1,p2)
    }

    def parseDefault(l:String,field:String):String = {
        val p1 = l.indexOf(field+":")
        if( p1 < 0 ) return "NULL"
        val p2 = l.indexOf(" ",p1+1)
        if( p2 < 0 ) return l.substring(p1+field.length+1)
        l.substring(p1+field.length+1,p2)
    }

    def parseDesc(l:String,field:String):String = {
        val p1 = l.indexOf(field+":")
        if( p1 < 0 ) return ""
        val s = l.substring(p1+field.length+1)
        s.replace("\"","&quot;")
    }

    def parseFieldName(t:String):String = {
        var l = t.trim()
        val p = l.indexOf(" ")
        if( p < 0 ) return l
        l.substring(0,p).trim
    }

}

object IdlReverseConverter {

    def main(args:Array[String]) {
        val p = args(0)
        val srcFile = "avenue_conf/" + p + ".xml"
        val targetFile = "aidl/" + p + ".aidl"
        convertFile(srcFile,targetFile)
    }

    val indent = "    "
    val indent2 = indent + indent
    val indent3 = indent + indent + indent

    def convertFile(srcFile:String,targetFile:String) {

        val cfgXml = XML.load(srcFile)

        val buff = ArrayBufferString()
        buff += "service:" + (cfgXml \ "@name").text + " id:" + (cfgXml \ "@id").text + " auto:1"

        buff += ""
        var l = cfgXml \ "type"
        for( n <- l ) {
            var name = (n \ "@name").text
            var cls = (n \ "@class").text
            val code = (n \ "@code").text
            val desc = (n \ "@desc").text
            if( cls != "array") {
                if( name.endsWith("_type") || name.endsWith("_Type") ) name = name.substring(0,name.length()-5)
                val clssuffix = if( cls != "string" ) ":"+cls else ""
                var s = "type:" + name+clssuffix + " code:" + code
                if( desc != "") s += " desc:"+desc
                buff += indent + s

                if( cls == "struct" ) {
                    val l2 = n \ "field"
                    for( n2 <- l2 ) {
                        var name = (n2 \ "@name").text
                        var tp = (n2 \ "@type").text
                        val tpsuffix = if( tp == "systemstring") "" else ":"+tp
                        var desc = (n2 \ "@desc").text
                        var s = indent2 + name + tpsuffix
                        if( desc != "") s += " desc:"+desc
                        buff += s
                    }
                }
            }
        }
        buff += ""
        l = cfgXml \ "message"
        for( n <- l ) {
            var name = (n \ "@name").text
            var id = (n \ "@id").text
            val desc = (n \ "@desc").text
            val uri = (n \ "@uri").text
            var s = "message:" + name + " id:" + id
            if( uri != "") s += " uri:"+uri
            if( desc != "") s += " desc:"+desc
            
            buff += indent + s
            buff += indent2 + "req:"

            val l2 = n \ "requestParameter" \ "field"
            for( n2 <- l2 ) {
                var name = (n2 \ "@name").text
                buff += indent3 + name
            }
            
            buff += indent2 + "res:"
            val l3 = n \ "responseParameter" \ "field"
            for( n3 <- l3) {
                var name = (n3 \ "@name").text
                buff += indent3 + name
            }

            buff += ""
        }

        println(buff.mkString("\n"))

        val s = buff.mkString("\n")
        val f = new File(targetFile)
        FileUtils.writeStringToFile(f,s,"UTF-8")
    }


}

