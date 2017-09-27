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
    通过此工具对指定目录下的flow重新格式化
*/

object FormatFlowTool {

    def help() {
        println(
"""
usage: scalabpe.FormatFlowTool [options] dirname
options:
    -h|--help               帮助信息
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

        processDir(dir,params)
    }

    def processDir(dir:String,params:HashMapStringAny) {
        val files = new File(dir).listFiles.filter(_.getName.endsWith(".flow"))
        for(f <- files ) {
            processFile(dir,f.getName,params)
        }
    }

    def processFile(dir:String,f:String,params:HashMapStringAny) {
        val lines = readAllLines(dir+File.separator+f)
        // TODO
    }

}


