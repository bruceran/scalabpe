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
    通过此工具对指定目录下的flow按format进行重命名
*/

object RenameFlowTool {

    var codecs : TlvCodecs = _

    def help() {
        println(
"""
usage: scalabpe.RenameFlowTool [options] dirname
options:
    -h|--help               帮助信息
    -f|--format [format]    文件名格式，可以使用servicename,msgname,serviceid,msgid 4个特殊字符串, 默认为 msgname_serviceid_msgid
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
                case "-f" | "--format" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("format",v)
                    i += skip
                case s if s.startsWith("-") => 
                    println("invalid option "+s)
                    return null
                case _ => 
                    files += args(i)
                    i += 1
            }
        }
        map.put("files",files)
        map
    }

    def main(args:Array[String]) {

        var params = parseArgs(args)
        if( params == null ) {
            help()
            return
        }
        var files = params.nls("files")
        if( files.size == 0 ) {
            help()
            return
        }

        var dir = files(0)
        if( !new File(dir).exists() ) {
            val p1 = "compose_conf"+File.separator+dir
            if( new File(p1).exists ) {
                dir = p1
            } else {
                println("not a valid dir, dir="+dir)
                return
            }
        }

        codecs = new TlvCodecs("avenue_conf")

        processDir(dir,params)
    }

    def processDir(dir:String,params:HashMapStringAny) {
        val files = new File(dir).listFiles.filter(_.getName.endsWith(".flow"))
        for(f <- files ) {
            processFile(dir,f.getName,params)
        }
    }

    def processFile(dir:String,f:String,params:HashMapStringAny) {
        val lines = readAllLines(dir+File.separator+f).filter(_.startsWith("//$"))
        if( lines.size == 0 ) {
            println("not a valid flow file, f="+f)
            return
        }
        val l = lines(0).substring(3)
        val p1 = l.indexOf(".")
        if( p1 < 0 ) {
            println("not a valid flow file, f="+f)
            return
        }
        val p2 = l.indexOf(".",p1+1)
        val service = if( p2 < 0 ) l else l.substring(0,p2).toLowerCase
        val ss = service.split("\\.")
        val serviceName = ss(0)
        val msgName = ss(1)
        val (serviceId,msgId) = codecs.serviceNameToId(service)
        if( serviceId == 0 || msgId == 0 ) {
            println("not a valid flow file, f="+f+",service="+service)
            return
        }
        var name = params.ns("format","msgname_serviceid_msgid")
        name = name.replace("serviceid",serviceId.toString)
        name = name.replace("msgid",msgId.toString)
        name = name.replace("servicename",serviceName)
        name = name.replace("msgname",msgName)
        val newfile = if(name.endsWith(".flow")) name else name + ".flow"
        if( newfile != f ) {
            new File(dir + File.separator + f).renameTo(new File(dir + File.separator + newfile))
            println("rename file, from="+f+",to="+newfile)
        }
    }

}


