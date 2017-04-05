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
    通过此工具比较服务描述文件，用于转发层和业务层的服务描述文件比较
*/

object CompareSdfTool {

    def help() {
        println(
"""
usage: scalabpe.CompareSdfTool [options] service1 service2
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

    }


}


