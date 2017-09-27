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

object Tools {

    val indent = "    "
    val indent2 = indent + indent
    val indent3 = indent + indent + indent

    val sep1 = 10001.toChar  // blank use to split txt params
    val sep2 = 10002.toChar  // : use to split txt params
    val sep3 = 10003.toChar  // new line
    val sep4 = 10004.toChar  // escape blank
    val sep5 = 10005.toChar  // escape :

    val r1 = """ ([a-zA-Z0-9_]+) *= *"([^"]*)"""".r

    def main(args:Array[String]) {
        println(
"""
此工具集包含以下工具：

scalabpe.GenSdfTool        在服务描述文件和等价的文本文件之间进行转换，自动补全type等操作
scalabpe.GenSqlTool        生成db服务描述文件
scalabpe.GenFlowTool       根据服务描述文件生成流程文件和http url mapping
scalabpe.RenameFlowTool    对流程文件进行按目录改名
""")
    }

    def parseNextParam(args:Array[String],i:Int):Tuple2[String,Int] = {
        if( i >= args.size ) return ("",1)
        val s = args(i)
        if( s.startsWith("-")) return ("",1)
        return (s,2)
    }

    def readAllLines(srcFile:String):Buffer[String] = {
        val src = Source.fromFile(srcFile,"UTF-8")
        val lines0 = src.getLines.map(_.replace("\t","    ")).map(_.trim).toBuffer
        src.close()
        lines0
    }
    def readAllLinesNoTrim(srcFile:String):Buffer[String] = {
        val src = Source.fromFile(srcFile,"UTF-8")
        val lines0 = src.getLines.map(_.replace("\t","    ")).toBuffer
        src.close()
        lines0
    }

    def escapeXml(v:String):String = {
        var s = v.replace("&","&amp;")
        s = s.replace("\"","&quot;")
        s = s.replace("'","&apos;")
        s = s.replace("<","&lt;")
        s = s.replace(">","&gt;")
        s
    }
    def unescapeXml(v:String):String = {
        var s = v.replace("&quot;","\"")
        s = s.replace("&apos;","'")
        s = s.replace("&lt;","<")
        s = s.replace("&gt;",">")
        s = s.replace("&amp;","&")
        s
    }

    def mergeXmlTags(lines:Buffer[String],tag1:String,tag2:String,sep:Char):ArrayBufferString = {
        val buff = ArrayBufferString()
        var merged = ""
        for( s <- lines ) {
            if( merged != "" ) {
                val p2 = s.lastIndexOf(tag2)
                if( p2 >= 0 ) {
                    val s1 = s.substring(0,p2+tag2.length).trim
                    val s2 = s.substring(p2+tag2.length).trim
                    merged += sep + s1
                    buff += merged
                    merged = ""
                    if( s2 != "" ) buff += s2
                } else {
                    merged += sep + s
                }
            } else {
                val p1 = s.indexOf(tag1)
                val p2 = s.lastIndexOf(tag2)
                if( p1 >= 0 && p2 >= 0 ) {
                    val s1 = s.substring(0,p1).trim
                    val s2 = s.substring(p1,p2+tag2.length).trim
                    val s3 = s.substring(p2+tag2.length).trim
                    if( s1 != "" ) buff += s1
                    if( s2 != "" ) buff += s2
                    if( s3 != "" ) buff += s3
                } 
                else if( p1 >= 0 ) {
                    val s1 = s.substring(0,p1).trim
                    val s2 = s.substring(p1).trim
                    if( s1 != "" ) buff += s1
                    merged = s2
                } else {
                    buff += s
                }
            }
        }
        buff
    }

    def parseAllXmlAttrs(l:String):LinkedHashMapStringAny = {
        val map = new LinkedHashMapStringAny()
        val i = r1.findAllMatchIn(l)
        for( m <- i ) {
            map.put(m.group(1),unescapeXml(m.group(2)))
        }
        map
    }
}


