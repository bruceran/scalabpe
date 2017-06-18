package scalabpe

import java.io._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.commons.io.FileUtils
import scala.xml._
import scala.collection.mutable._
import scalabpe.core._
import org.apache.commons.lang.StringUtils
import Tools._

/*
    通过此工具一次生成整个服务描述文件的flow框架性代码
*/

object GenFlowTool {

    def help() {
        println(
"""
usage: scalabpe.GenFlowTool [options]
options:
    -h|--help                       帮助信息
    -s|--sdf  servicefile           指定服务文件名
    -d|--dir  dirname               指定目录，默认为服务文件名一致
    -w|--with withname              所有流程需要继承的基类
    -i|--import                     自动加上import FlowHelper._
    -m|--mapping                    根据uri,uri2,needLogin生成url mapping
    -p|--plugin [pluginname]        -m开启时使用，可给url mapping加上plugin参数
""")
    }

    def parseArgs(args:Array[String]):HashMapStringAny = {
        val map = HashMapStringAny()
        var i = 0
        val files = ArrayBufferString()
        while(i < args.size) {
            args(i) match {
                case "-h" | "--help" => 
                    return null
                case "-s" | "--sdf" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("service",v)
                    i += skip
                case "-d" | "--dir" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("dir",v)
                    i += skip
                case "-w" | "--with" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("with",v)
                    i += skip
                case "-p" | "--plugin" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("plugin",if( v=="" ) "xxx" else v)
                    i += skip
                case "-i" | "--import" => 
                    map.put("import","1")
                    i += 1
                case "-m" | "--mapping" => 
                    map.put("mapping","1")
                    i += 1
                case s if s.startsWith("-") => 
                    println("invalid option "+s)
                    return null
                case _ => 
                    println("invalid option "+args(i))
                    return null
            }
        }
        map
    }

    def main(args:Array[String]) {

        var params = parseArgs(args)
        if( params == null ) {
            help()
            return
        }

        val filename = findFile(params.ns("service"))
        if( filename == null ) {
            println("file not found, service="+params.ns("service"))
            help()
            return
        }

        processFile(filename,params)
    }

    def findFile(s:String):String = {
        s match {
            case s if s.toLowerCase.endsWith(".xml") =>
                if( new File(s).exists() ) {
                    return s
                }
                val p1 = "avenue_conf"+File.separator+s
                if( new File(p1).exists ) {
                    return p1
                }
                return null
            case s  =>
                return null
        }
    }

    def processFile(filename:String,params:HashMapStringAny) {

        val codec = new TlvCodec(filename)

        var dir = params.ns("dir")
        if( dir == "" ) {
            val f = new File(filename)
            dir = "compose_conf"+File.separator+f.getName.substring(0,f.getName.length-4)
        } else {
            if( !dir.startsWith("compose_conf") ) {
                dir ="compose_conf"+File.separator+dir 
            }
        }
        if( !new File(dir).exists ) {
            new File(dir).mkdirs()
        }

        for( msgId <- codec.msgIds ) {
            val msgName = codec.msgIdToNameMap.getOrElse(msgId,"unknown")
            generateFile(dir,codec.serviceId.toString,codec.serviceName,msgId.toString,msgName,params,codec.msgAttributes.getOrElse(msgId,null))
        }
    }
    
    def generateFile(dir:String,serviceid:String,servicename:String,msgid:String,msgname:String,params:HashMapStringAny,attrs:HashMapStringString) {

        val _with = params.ns("with")
        val _import = params.ns("import")
        var _plugin = params.ns("plugin")

        var filename = dir + File.separator + msgname + "_"+serviceid +"_"+msgid+".flow"

        var buff = ArrayBufferString()

        var s  = "//$"+servicename.toLowerCase+"."+msgname.toLowerCase
        if( _with != "" ) s += ".with("+_with+")"
        buff += s
        buff += ""

        if( _import == "1" ) {
            buff += "import FlowHelper._"
            buff += ""
        }

        var transfer = ""
        if( attrs != null ) {
            transfer = attrs.getOrElse("transfer","")
            if( transfer != "" )
                buff += "toMsgName=\""+transfer+"\""
        }

        buff += ""
        if( transfer == "" ) {
            buff += "//#receive"
            buff += ""
            buff += indent + "reply(0)"
        } else {

        }

        //println(buff.mkString("\n"))

        s = buff.mkString("\n")
        val f = new File(filename)
        FileUtils.writeStringToFile(f,s,"UTF-8")

        if( attrs == null ) return
        if( params.ns("mapping") != "1" ) return // 生成url mapping

        var uri = attrs.getOrElse("uri","")
        var uri2 = attrs.getOrElse("uri2","")
        var needLogin = attrs.getOrElse("needLogin","")

        if( uri != "" ) {
            s = """<Item plugin="%s" login="%s">%s,%s,%s</Item>""".format(_plugin,needLogin,uri,serviceid,msgid)
            s = s.replace(" login=\"\"","")
            s = s.replace(" plugin=\"\"","")
            println(s)
        }
        if( uri2 != "" ) {
            s = """<Item plugin="%s" login="%s">%s,%s,%s</Item>""".format(_plugin,needLogin,uri2,serviceid,msgid)
            s = s.replace(" login=\"\"","")
            s = s.replace(" plugin=\"\"","")
            println(s)
        }
    }

}


