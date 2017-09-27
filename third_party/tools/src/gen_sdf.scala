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
    手工编写xml格式的服务描述文件比较麻烦; 通过此辅助工具可以在xml和等价的txt文件之间互相转换;
*/

// todo 合并field里的desc和type里相同的desc, 不相同的保留

object GenSdfTool {

    val r2 = """ ([^ :]+):""".r

    class Context {
        var lastNode:String = ""
        var arrayName:String = ""
        var itemName:String = ""
    }

    var messageIdSeq = 0
    var typeCodeSeq = 0
    var version = 1

    def help() {
        println(
"""
usage: scalabpe.GenSdfTool [options] path_to_filename(xml|txt)
options:
    -h|--help             帮助信息
    -f|--format           重新格式化xml/txt文件, 格式完毕后会删除中间文件
    -r|--repair           自动补全缺少的type定义, 默认按string类型补定义
       --reset            xml转txt时删除所有的code和id,用于准备重新对参数排序的情况
    -d|--delete           转换完毕后删除原始文件
    -c|--console          转换完毕后输出结果到控制台
    -o|--output filename  生成到指定文件中
       --no_default_type  不输出符合默认规则的type定义
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
                case "-d" | "--delete" => 
                    map.put("delete","1")
                    i += 1
                case "-f" | "--format" => 
                    map.put("format","1")
                    i += 1
                case "--reset" => 
                    map.put("reset","1")
                    i += 1
                case "--no_default_type" => 
                    map.put("no_default_type","1")
                    i += 1
                case "-r" | "--repair" => 
                    map.put("repair","1")
                    i += 1
                case "-c" | "--console" => 
                    map.put("console","1")
                    i += 1
                case "-o" | "--output" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("output",v)
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

        var basepath = ""
        var p0 = files(0)
        if( !new File(p0).exists() ) {
            val p1 = "avenue_conf"+File.separator+p0
            if( new File(p1).exists ) {
                p0 = p1
                basepath = "avenue_conf"+File.separator
            }
        }
        
        println("process file: "+p0)

        if( p0.endsWith(".xml") ) {
            var p2 = p0.replace(".xml",".txt")
            if( params.ns("output") != "" ) p2 = params.ns("output")
            val tmp_p2 = p2 + ".tmp"
            val ok = convertXmlToTxt(p0,tmp_p2,params)
            if( ok && params.ns("format") == "1" ) {
                convertTxtToXml(tmp_p2,p0,params)
                new File(tmp_p2).delete()
                return
            }
            compareAndRename(p2,tmp_p2)
            if( ok && params.ns("delete") == "1" ) {
                new File(p0).delete()
            }
            return
        }

        if( p0.endsWith(".txt") ) {
            var p2 = p0.replace(".txt",".xml")
            if( params.ns("output") != "" ) p2 = basepath + params.ns("output")
            val tmp_p2 = p2 + ".tmp"
            val ok = convertTxtToXml(p0,tmp_p2,params)
            if( ok && params.ns("format") == "1" ) {
                convertXmlToTxt(tmp_p2,p0,params)
                new File(tmp_p2).delete()
                return
            }

            compareAndRename(p2,tmp_p2)

            if( ok && params.ns("delete") == "1" ) {
                new File(p0).delete()
            }
            return
        }

        println("not a valid file, file="+p0)
    }

    def compareAndRename(p2:String,tmp_p2:String) {
        val f2 = new File(p2)
        if( f2.exists ) {
            if( md5file(p2) == md5file(tmp_p2) ) {
                new File(tmp_p2).delete()
                return
            }
            new File(p2).delete()
        }
        new File(tmp_p2).renameTo(new File(p2))
    }

    def md5file(file:String):String = {
        val s = FileUtils.readFileToString(new File(file),"UTF-8")
        CryptHelper.md5(s)
    }

    def convertTxtToXml(srcFile:String,targetFile:String,params:HashMapStringAny):Boolean = {
        val lines0 = readAllLines(srcFile)
        var lines = prepareTxtLines(lines0)
        lines = repairTxtTypes(lines,params.ns("repair") == "1")
        val typeDescMap = parseTypeDesc(lines)
        
        val no_default_type = params.ns("no_default_type") == "1"

        val buff = ArrayBufferString()
        buff += """<?xml version="1.0" encoding="UTF-8"?>"""
        val ctx = new Context()
        var lineNo = 0
        for( s <- lines ) {
            lineNo += 1
            try {
                s match {
                    case l if l.startsWith("service:") =>
                        convertService(buff,l,ctx)
                    case l if l.startsWith("type:") =>
                        convertType(buff,l,ctx)
                    case l if l.startsWith("type_end:") =>
                        convertTypeEnd(buff,l,ctx)
                    case l if l.startsWith("message:") =>
                        convertMessage(buff,l,ctx)
                    case l if l.startsWith("sql:") =>
                        convertSql(buff,l,ctx)
                    case l if l.startsWith("req:") =>
                        convertReq(buff,l,ctx)
                    case l if l.startsWith("res:") =>
                        convertRes(buff,l,ctx)
                    case l if l.startsWith("res_end:") =>
                        convertResEnd(buff,l,ctx)
                    case l if l.startsWith("#") =>
                        convertComment(buff,l,ctx)
                    case l if l == "" =>
                        buff += ""
                    case _ =>
                        convertField(buff,s,ctx,typeDescMap,no_default_type)
                }
            } catch {
                case e:Throwable =>
                    println(buff.mkString("\n"))
                    println("line#"+lineNo+", "+e.getMessage)
                    return false
            }
        }
        val lastline = buff(buff.size-1)
        if( lastline != "" ) {
            buff += ""
        }
        buff += "</service>"

        if( params.ns("console") == "1" ) {
            println(buff.mkString("\n"))
        }

        val s = buff.mkString("\n")
        val f = new File(targetFile)
        FileUtils.writeStringToFile(f,s,"UTF-8")

        try {
            val codec = new TlvCodec(targetFile)
            println("xml is valid")
            return true
        } catch {
            case e:Throwable =>
                println("xml is not valid, error="+e.getMessage)
                return false
        }
    }

    def parseTypeDesc(lines:ArrayBufferString):HashMapStringAny = {
        val map = HashMapStringAny()
        for( l <- lines ) {
            if( l.startsWith("type:") ) {
                val m = parseMap(l)
                var name = m.ns("type")
                var desc = m.ns("desc")
                map.put(name.toLowerCase,desc)
            }
        }
        map
    }

    def prepareTxtLines(list0:Buffer[String]):ArrayBufferString = {
        genEndLabel(mergeSqls(mergeComments(list0)))
    }

    def repairTxtTypes(lines:Buffer[String],repair:Boolean):ArrayBufferString = {
        val typenames = ArrayBufferString()
        val missedtypes = new LinkedHashMapStringAny()
        val notusedtypes = new LinkedHashMapStringAny()

        for( l <- lines ) {
            if( l.startsWith("type:") ) {
                val m = parseMap(l)
                var name = m.ns("type")
                val p = name.indexOf("#")
                var cls = "string"
                if( p > 0 ) {
                    cls = name.substring(p+1)
                    name = name.substring(0,p)
                }
                typenames += name.toLowerCase
                notusedtypes.put(name.toLowerCase,"0")
                var array = m.ns("array")
                if( array != "" ) {
                    val arrayName = nameToArrayName(name,array)
                    typenames += arrayName.toLowerCase
                    notusedtypes.put(arrayName.toLowerCase,"0")
                    notusedtypes.remove(name.toLowerCase)
                }
            }
        }

        var context = ""
        for( l <- lines ) {
            if( context != "" && l != "" && !l.startsWith("#")  && !l.startsWith("req:") && !l.startsWith("res:") && !l.startsWith("res_end:")  && !l.startsWith("type_end:")   ) {
                val name = parseFieldName(l)
                val m = parseMap(l)
                var tp = m.ns("type",name+"_type")
                var desc = m.ns("desc")
                if( !typenames.contains(tp.toLowerCase) ) {
                    val desc2 = missedtypes.ns(tp)
                    if( desc == "" && desc2 != "" ) desc = desc2
                    missedtypes.put(tp,desc)
                }
                notusedtypes.put(tp.toLowerCase,"1")
            }

            if( l.startsWith("type:") ) {
                val m = parseMap(l)
                var name = m.ns("type")
                val p = name.indexOf("#")
                var cls = "string"
                if( p > 0 ) {
                    cls = name.substring(p+1)
                }
                if( cls == "object") {
                    context = "object"
                }
            }
            if( l.startsWith("req:") ) {
                context = "req"
            }
            if( l.startsWith("res:") ) {
                context = "res"
            }
            if( l.startsWith("res_end:") ) {
                context = ""
            }
            if( l.startsWith("type_end:") ) {
                context = ""
            }
        }

        if( true ) {
            println("--- not used types ---")
            for( (k,v) <- notusedtypes if v == "0" ) {
                    println(k)
            }
            println("--- not used types end ---")
        }

        if(!repair) { 
            if( missedtypes.size > 0 ) {
                println("--- missed types ---")
                for( (k,v) <- missedtypes ) {
                    var s = "type:"+k
                    if( v != "") s += " desc:"+v
                    println(s)
                }
                println("--- missed types end ---")
            }
            val buff = ArrayBufferString()
            for(l <-lines ) buff += l
            return buff
        }

        val buff = ArrayBufferString()
        var firstMessageFound = false
        for( l <- lines ) {
            if( l.startsWith("message:") && !firstMessageFound ) {
                if( missedtypes.size > 0 ) {
                    buff += "#missed types"
                    for( (k,v) <- missedtypes ) {
                        var s = "type:"+k
                        if( v != "") s += " desc:"+v
                        buff += s
                    }
                    buff += "#missed types end"
                    buff += ""

                    firstMessageFound = true
                }
            }
            buff += l
        }
        buff
    }

    def genEndLabel(lines:Buffer[String]):ArrayBufferString = {
        val buff = ArrayBufferString()
        var i = 0
        while( i < lines.size ) {
            val s = lines(i)
            buff += s
            if( s.startsWith("type:") && s.indexOf("#struct") > 0 ) {
                val j = findEnd(lines,i+1)
                for( k <- i+1 until j ) {
                    buff += lines(k)
                }
                buff += "type_end:"
                i = j - 1
            } else if( s.startsWith("type:") && s.indexOf("#object") > 0 ) {
                val j = findEnd(lines,i+1)
                for( k <- i+1 until j ) {
                    buff += lines(k)
                }
                buff += "type_end:"
                i = j - 1
            } else if( s.startsWith("res:") ) {
                val j = findEnd(lines,i+1)
                for( k <- i+1 until j ) {
                    buff += lines(k)
                }
                buff += "res_end:"
                i = j - 1
            } 

            i += 1
        }
        buff
    }

    def findEnd(lines:Buffer[String],i:Int):Int = {
        var found = -1
        var k = i
        while( k < lines.size && found == -1 ) {
            val s = lines(k)
            if( s.startsWith("type:") || s.startsWith("message:") || s.startsWith("sql:") ) {
               found = k 
            }
            k += 1
        }
        if( found == -1 ) found = lines.size
        var found2 = -1
        k = found - 1
        while( k > 0 && found2 == -1 ) {
            val s = lines(k)
            if( s != "" && !s.startsWith("#") ) {
               found2 = k 
            }
            k -= 1
        }
        if( found2 == -1 ) found2 = lines.size
        found2 + 1
    }

    def mergeComments(lines:Buffer[String]):ArrayBufferString = {
        val buff = ArrayBufferString()
        var merged = ""
        for( s <- lines ) {
            if( merged != "" ) {
                merged += sep3 + s
                if( s.endsWith("###") ) { // 以###结尾
                    buff += merged
                    merged = ""
                } 
            } else {
                if( s.startsWith("###") ) { // 以###开始
                    merged = s
                } else { 
                    buff += s 
                }
            }
        }
        buff
    }

    def mergeSqls(lines:Buffer[String]):ArrayBufferString = {
        val buff = ArrayBufferString()
        var merged = ""
        for( s <- lines ) {
            if( merged != "" ) {
                if( s == "req:" ) {
                    buff += merged
                    merged = ""
                    buff += s
                } else {
                    merged += sep3 + s
                }
            } else {
                if( s == "sql:" ) {
                    merged = s
                } else { 
                    buff += s 
                }
            }
        }
        buff
    }

    def convertService(buff:ArrayBufferString,l:String,ctx:Context) {
        val map = parseMap(l)
        val name = map.ns("service")
        val id = map.ns("id")
        version = map.ns("version","1").toInt
        map.remove("service")
        map.remove("id")
        var extra = ""
        for( (k,v) <- map ) {
            extra += " " + k + "=\"" +escapeXml(v.toString)+ "\""
        }
        val s = """<service name="%s" id="%s" IsTreeStruct="false"%s>""".format(name,id,extra)
        buff += s 
    }

    def convertType(buff:ArrayBufferString,l:String,ctx:Context) {
        val map = parseMap(l)
        var name = map.ns("type")
        var cls = "string"
        val p = name.indexOf("#")
        if( p > 0 ) {
            cls = name.substring(p+1)
            name = name.substring(0,p)
        }
        var code = map.ns("code")
        if( code == "" ) {
            typeCodeSeq += 1
            code = typeCodeSeq.toString
        } else {
            typeCodeSeq = code.toInt
        }

        var array = map.ns("array")

        map.remove("type")
        map.remove("class")
        map.remove("code")
        map.remove("array")

        var extra = ""
        for( (k,v) <- map ) {
            extra += " " + k + "=\"" +escapeXml(v.toString)+ "\""
        }
        var closetag = "/"
        if( cls == "struct" ) { ctx.lastNode = "struct" ; closetag = "" } 
        if( cls == "object" ) { ctx.lastNode = "object" ; closetag = "" } 

        var xml = indent + StringUtils.rightPad("<type name=\"%s\"".format(name),40,' ') + 
                StringUtils.rightPad(" class=\"%s\"".format(cls),16,' ') + 
                " code=\"%s\"".format(code) + extra + closetag + ">"

        if( array != "" ) {
            val arrayName = nameToArrayName(name,array)
            if( cls == "struct" || cls == "object" ) {
                ctx.arrayName = arrayName
                ctx.itemName = name
            } else {
                val s = indent + StringUtils.rightPad("<type name=\"%s\"".format(arrayName),40,' ') + 
                    StringUtils.rightPad(" class=\"array\"",16,' ') + 
                    " itemType=\"%s\"".format(name) + "/>"
                xml =  xml + "\n" + s
            }
        }
        buff += xml
    }

    def convertTypeEnd(buff:ArrayBufferString,l:String,ctx:Context) { 
        var s = indent + "</type>"
        if( ctx.arrayName != "") {
            s += "\n" + indent + StringUtils.rightPad("<type name=\"%s\"".format(ctx.arrayName),40,' ') + 
                    StringUtils.rightPad(" class=\"array\"",16,' ') + 
                    " itemType=\"%s\"".format(ctx.itemName) + "/>"
        }
        buff += s 

        ctx.lastNode = ""
        ctx.arrayName = ""
        ctx.itemName = ""
    }

    def convertMessage(buff:ArrayBufferString,l:String,ctx:Context) {

        val map = parseMap(l)
        var name = map.ns("message")
        var id = map.ns("id")

        map.remove("message")
        map.remove("id")

        if( id == "" ) {
            messageIdSeq += 1
            id = messageIdSeq.toString
        } else {
            messageIdSeq = id.toInt
        }

        var extra = ""
        for( (k,v) <- map ) {
            extra += " " + k + "=\"" +escapeXml(v.toString)+ "\""
        }
        var s = indent + """<message name="%s" id="%s"%s>""".format(name,id,extra)
        buff += s
        ctx.lastNode = "message"
    }

    def convertSql(buff:ArrayBufferString,l:String,ctx:Context) {
        if( ctx.lastNode == "message" ) {
            var ns = l.substring(4).trim.replaceFirst("^"+sep3+"*","").replaceFirst(sep3+"*$","")
            val lines = ns.split(sep3)
            buff += indent2 + "<sql><![CDATA[" 
            for( s <- lines ) 
                buff += indent3 + s
            buff += indent2 + "]]></sql>" 
        } else {
            var s = indent + "<sql><![CDATA[%s]]></sql>".format(l.substring(4))
            buff += s 
        }
    }

    def convertReq(buff:ArrayBufferString,l:String,ctx:Context) {
        val s = indent2 + "<requestParameter>"
        buff += s 
        ctx.lastNode = "req"
    }

    def convertRes(buff:ArrayBufferString,l:String,ctx:Context) {
        val s = indent2 + "</requestParameter>\n" + indent2 + "<responseParameter>" 
        buff += s 
        ctx.lastNode = "res"
    }

    def convertResEnd(buff:ArrayBufferString,l:String,ctx:Context) { 
        val s = ""+indent2 + "</responseParameter>\n" + indent + "</message>"
        buff += s 
        ctx.lastNode = ""
        ctx.arrayName = ""
        ctx.itemName = ""
    }

    def convertComment(buff:ArrayBufferString,l:String,ctx:Context) {

        var ns = l.trim.replaceFirst("^#*","").replaceFirst("#*$","")
        ns = ns.trim.replaceFirst("^"+sep3+"*","").replaceFirst(sep3+"*$","")
        val lines = ns.split(sep3)

        val ind = ctx.lastNode match {
            case "struct" => indent2
            case "object" => indent2
            case "message" => indent2
            case "req" => indent3
            case "res" => indent3
            case _ => indent
        }
        if( lines.size == 1 ) {
            val s = ind + "<!-- " + lines(0) + " -->"
            buff += s 
        } else {
            buff += ind + "<!--" 
            for( s <- lines ) 
                buff += ind + s
            buff += ind + "-->" 
        }
    }

    def convertField(buff:ArrayBufferString,l:String,ctx:Context,typeDescMap:HashMapStringAny,no_default_type:Boolean) {

        var name = parseFieldName(l)
        var p = l.indexOf(" ") 
        var ns = if( p == -1 ) "" else l.substring(p+1)
        val map = parseMap(ns)

        var tp = map.ns("type")
        map.remove("type")

        ctx.lastNode match {
            case "req" | "res" =>
                val desc = map.ns("desc")
                val desc2 = typeDescMap.ns(tp.toLowerCase)
                if( desc2 == desc ) map.remove("desc")

                var extra = ""
                for( (k,v) <- map ) {
                    extra += " " + k + "=\"" +escapeXml(v.toString)+ "\""
                }

                var xml = ""
                if( tp == "" && no_default_type ) {
                    xml = indent3 + "<field name=\"%s\"".format(name) + extra+"/>" 
                } else {
                    if( tp == "" ) tp = name + "_type"
                    xml = indent3 + StringUtils.rightPad("<field name=\"%s\"".format(name),32,' ') +
                            " type=\"%s\"".format(tp) + extra+"/>" 
                }
                buff += xml
            case "struct" =>
                tp = "systemstring" 
                val p = name.indexOf("#")
                if( p > 0 ) {
                    tp = name.substring(p+1)
                    name = name.substring(0,p)
                }

                var extra = ""
                for( (k,v) <- map ) {
                    extra += " " + k + "=\"" +escapeXml(v.toString)+ "\""
                }

                var xml = indent2 + StringUtils.rightPad("<field name=\"%s\"".format(name),36,' ') +
                        " type=\"%s\"".format(tp) + extra+"/>" 
                buff += xml
            case "object" =>

                var extra = ""
                for( (k,v) <- map ) {
                    extra += " " + k + "=\"" +escapeXml(v.toString)+ "\""
                }

                var xml = ""
                if( tp == "" && no_default_type ) {
                    xml = indent2 + "<field name=\"%s\"".format(name) + extra+"/>" 
                } else {
                    if( tp == "" ) tp = name + "_type"
                    xml = indent2 + StringUtils.rightPad("<field name=\"%s\"".format(name),36,' ') +
                            " type=\"%s\"".format(tp) + extra+"/>" 
                }
                buff += xml
            case _ =>
                throw new Exception("unknown line:"+l)
        }
    }

    def parseFieldName(t:String):String = {
        var l = t.trim()
        val p = l.indexOf(" ")
        if( p < 0 ) return l
        l.substring(0,p).trim
    }

    def parseMap(s:String):LinkedHashMapStringAny = {
        val map = new LinkedHashMapStringAny()

        var desc = ""
        var ns = s
        val p = s.indexOf("desc:")
        if( p > 0 )  {
            desc = s.substring(p+5)
            ns = s.substring(0,p)
        }
        ns = escape(ns)

        ns = r2.replaceAllIn(" "+ns,(m)=>sep1+m.group(1).replace("$","\\$")+sep2) // $0 ... $9 有特殊含义，$需转义
        val ss = ns.split(sep1).map(_.trim)
        for( t <- ss ) {
            val tt = t.split(sep2)
            var key = tt(0)
            if( tt.size >= 2 ) map.put(key,unescape(tt(1)))
            else if( key != "") map.put(key,"")
        }
        if( desc != "" ) map.put("desc",desc)
        map
    }

    def convertXmlToTxt(srcFile:String,targetFile:String,params:HashMapStringAny):Boolean = {
        try {
            XML.load(srcFile)
        } catch {
            case e:Throwable =>
                println("xml cannot be loaded, message="+e.getMessage)
                return false
        }

        val lines0 = readAllLines(srcFile)
        var lines = parseXmlLines(lines0)
        lines = repairXmlTypes(lines,params.ns("repair") == "1")

        val resetFlag = params.ns("reset") == "1"

        val buff = ArrayBufferString()
        var context = ""
        var comment_indent = indent
        val arrayMap = parseXmlArray(lines)
        var lastCode = "0"
        var lastMessageId = "0"
        for( l <- lines ) {

                    l match {
                        case l if l.startsWith("<!--") =>
                            val cs = parseMergedXmlTags(l,"<!--","-->")
                            if( cs.size == 1 ) {
                                buff += comment_indent + "#" + cs(0)
                            } else {
                                buff += comment_indent + "###"
                                for( c <- cs )
                                    buff += comment_indent + c
                                buff += comment_indent + "###"
                            }
                        case l if l.startsWith("<sql") =>
                            val l2 = l.replace("<![CDATA[","").replace("]]>","")
                            val cs = parseMergedXmlTags(l2,"<sql>","</sql>")
                            val ind = if( context != "message" ) indent else indent2
                            if( cs.size == 1 ) {
                                buff += ind +"sql:"+cs(0)
                            } else {
                                buff += ind+"sql:"
                                for( c <- cs )
                                    buff += ind +indent + c
                            }
                        case l if l == "" =>
                            buff += ""

                        case l if l.startsWith("<service") =>
                            val attrs = parseAllXmlAttrs(l)
                            var name = attrs.ns("name")
                            val id = attrs.ns("id")
                            version = attrs.ns("version","1").toInt
                            val desc = attrs.getOrElse("desc",null)
                            attrs.remove("name")
                            attrs.remove("id")
                            attrs.remove("isTreeStruct")
                            attrs.remove("IsTreeStruct")
                            attrs.remove("desc")
                            var s = "service:"+name+indent+"id:"+id
                            for( (k,v) <- attrs ) s += indent+k+":"+escape0(v.toString)
                            if( desc != null ) 
                                s += indent + "desc:"+desc
                            buff += s

                        case l if l.startsWith("<type") =>
                            val attrs = parseAllXmlAttrs(l)
                            var name = attrs.ns("name")
                            val code = attrs.ns("code")
                            val cls = attrs.ns("class")
                            val desc = attrs.getOrElse("desc",null)
                            attrs.remove("name")
                            attrs.remove("code")
                            attrs.remove("class")
                            attrs.remove("desc")

                            if( cls != "array" ) {
                                var s = indent+"type:"+name
                                if( cls != "string" ) s += "#"+cls 
                                s = StringUtils.rightPad(s,40,' ')
                                if(!resetFlag) {
                                    if( code.toInt != lastCode.toInt + 1 )
                                        s += indent+"code:"+code
                                }
                                lastCode = code
                                val arrayName = arrayMap.ns(name.toLowerCase)
                                if( arrayName != "" ) 
                                    s += indent + "array:"+arrayName
                                for( (k,v) <- attrs ) s += indent+k+":"+escape0(v.toString)
                                if( desc != null ) 
                                    s += indent + "desc:"+desc
                                if( cls  == "struct" ) {
                                    context = "struct"
                                    comment_indent = indent2
                                }
                                if( cls  == "object" ) {
                                    context = "object"
                                    comment_indent = indent2
                                }
                                buff += s 
                            }
                        case l if l.startsWith("<field") =>
                            val attrs = parseAllXmlAttrs(l)
                            var name = attrs.ns("name")
                            var tp = attrs.ns("type")
                            val desc = attrs.getOrElse("desc",null)
                            attrs.remove("name")
                            attrs.remove("type")
                            attrs.remove("desc")
                            if( context == "struct" ) {
                                var s = indent2+name
                                if( tp != "systemstring" ) s +="#"+tp 
                                s = StringUtils.rightPad(s,40,' ')
                                for( (k,v) <- attrs ) s += indent+k+":"+escape0(v.toString)
                                if( desc != null ) 
                                    s += indent + "desc:"+desc
                                buff += s
                            }
                            if( context == "object" ) {
                                var s = indent2+name
                                s = StringUtils.rightPad(s,40,' ')
                                if( tp.toLowerCase != (name+"_type").toLowerCase ) s += indent + "type:"+tp 
                                for( (k,v) <- attrs ) s += indent+k+":"+escape0(v.toString)
                                if( desc != null ) 
                                    s += indent + "desc:"+desc
                                buff += s
                            }
                            if( context == "message" ) {
                                if( tp == "" ) tp = name + "_type"
                                var s = ""
                                if( tp.toLowerCase == (name + "_type").toLowerCase ) 
                                    s = indent3+name
                                else
                                    s = indent3+name+indent+"type:"+tp
                                s = StringUtils.rightPad(s,40,' ')
                                for( (k,v) <- attrs ) s += indent+k+":"+escape0(v.toString)
                                if( desc != null ) 
                                    s += indent + "desc:"+desc
                                buff += s
                            }
                        case l if l.startsWith("<message") =>
                                val attrs = parseAllXmlAttrs(l)
                                var name = attrs.ns("name")
                                var id = attrs.ns("id")
                                val desc = attrs.getOrElse("desc",null)
                                attrs.remove("name")
                                attrs.remove("id")
                                attrs.remove("desc")

                                var s = indent+"message:"+name
                                if(!resetFlag) {
                                    if( id.toInt != lastMessageId.toInt + 1 )
                                        s += indent+"id:"+id
                                }
                                lastMessageId = id
                                for( (k,v) <- attrs ) s += indent+k+":"+escape0(v.toString)
                                if( desc != null ) 
                                    s += indent + "desc:"+desc
                                buff += s 
                                context = "message"
                                comment_indent = indent2
                        case l if l.startsWith("<requestParameter") =>
                                    buff += indent2+"req:"
                                    comment_indent = indent3
                        case l if l.startsWith("</requestParameter") =>
                                    comment_indent = indent2
                        case l if l.startsWith("<responseParameter") =>
                                    buff += indent2+"res:"
                                    comment_indent = indent3
                        case l if l.startsWith("</responseParameter") =>
                                    comment_indent = indent2
                        case l if l.startsWith("</message") =>
                                    comment_indent = indent
                        case l if l.startsWith("</type") =>
                                    comment_indent = indent
                        case _ =>
                    }
        }

        if( params.ns("-c") == "1" ) {
            println(buff.mkString("\n"))
        }

        val s = buff.mkString("\n")
        val f = new File(targetFile)
        FileUtils.writeStringToFile(f,s,"UTF-8")

        true
    }

    def parseXmlArray(lines:ArrayBufferString):HashMapStringAny = {
        val map = HashMapStringAny()
        for( l <- lines ) {
            if( l.startsWith("<type ") ) {
                val attrs = parseAllXmlAttrs(l)
                var cls = attrs.ns("class")
                if( cls == "array" ) {
                    val itemType = attrs.ns("itemType").toLowerCase
                    var name = attrs.ns("name")
                    map.put(itemType,name)
                }
            }
        }
        map
    }

    def repairXmlTypes(lines:Buffer[String],repair:Boolean):ArrayBufferString = {
        val typenames = ArrayBufferString()
        val missedtypes = new LinkedHashMapStringAny()
        var maxCode = 0
        for( l <- lines ) {
            if( l.startsWith("<type ") ) {
                val m = parseAllXmlAttrs(l)
                var name = m.ns("name")
                typenames += name.toLowerCase
                var cls = m.ns("class")
                if( cls != "array") {
                    var code = m.ns("code").toInt
                    if( code > maxCode ) maxCode = code
                }
            }
        }

        var context = ""
        for( l <- lines ) {
            if( context != "" && l.startsWith("<field") ) {
                val m = parseAllXmlAttrs(l)
                val name = m.ns("name")
                val tp = m.ns("type",name+"_type")
                var desc = m.ns("desc")

                if( !typenames.contains(tp.toLowerCase) ) {
                    val desc2 = missedtypes.ns(tp)
                    if( desc == "" && desc2 != "" ) desc = desc2
                    missedtypes.put(tp,desc)
                }
            }

            if( l.startsWith("<requestParameter") ) {
                context = "req"
            }
            if( l.startsWith("</responseParameter") ) {
                context = ""
            }
            if( l.startsWith("<responseParameter") ) {
                context = "res"
            }
            if( l.startsWith("</responseParameter") ) {
                context = ""
            }
        }

        if(!repair) { 
            if( missedtypes.size > 0 ) {
                println("--- missed types ---")
                for( (k,v) <- missedtypes ) {
                    if( v != "")
                        println("""<type name="%s" class="string" code="%d" desc="%s"/>""".format(k,maxCode,v))
                    else
                        println("""<type name="%s" class="string" code="%d"/>""".format(k,maxCode))
                    maxCode += 1
                }
                println("--- missed types end ---")
            }
            val buff = ArrayBufferString()
            for(l <-lines ) buff += l
            return buff
        }

        val buff = ArrayBufferString()
        var firstMessageFound = false
        for( l <- lines ) {
            if( l.startsWith("<message ") && !firstMessageFound ) {
                if( missedtypes.size > 0 ) {
                    buff += "<!-- missed types -->"
                    for( (k,v) <- missedtypes ) {
                        var s = """<type name="%s" class="string" code="%d"/>""".format(k,maxCode)
                        if( v != "")
                            s = """<type name="%s" class="string" code="%d" desc="%s"/>""".format(k,maxCode,v)
                        buff += s
                        maxCode += 1
                    }
                    buff += "<!-- missed types end --> "
                    buff += ""

                    firstMessageFound = true
                }
            }
            buff += l
        }
        buff
    }

    def parseXmlLines(list0:Buffer[String]):ArrayBufferString = {
        val list1 = mergeXmlTags(list0,"<!--","-->",sep3)
        val list2 = mergeXmlTags(list1,"<sql>","</sql>",sep3)
        val list3 = mergeXmlTags(list2,"<",">",' ')
        list3
    }

    def parseMergedXmlTags(l:String,tag1:String,tag2:String):Array[String] = {
        var p1 = l.indexOf(tag1)
        if( p1 < 0 ) p1 = 0
        else p1 += tag1.length
        var p2 = l.lastIndexOf(tag2)
        if( p2 < 0 ) p2 = l.length
        val s = l.substring(p1,p2).trim
        var ns = s.trim.replaceFirst("^"+sep3+"*","").replaceFirst(sep3+"*$","")
        ns.split(sep3)
    }

    def escape0(s:String):String = {
        s.replace(":","\\:")
    }
    
    def escape(s:String):String = {
        var afterSlash = false
        var ts = ""
        for(c <- s) {
            if( afterSlash ) {
                c match {
                    case ' ' =>
                        ts = ts + sep4
                    case ':' =>
                        ts = ts + sep5
                    case _ =>
                        ts = ts + '\\'
                        ts = ts + c
                }
                afterSlash = false
            } else if( c == '\\')  { 
                afterSlash = true
            } else {
                ts = ts + c
            }
        }
        if( afterSlash )
            ts = ts + '\\'
        ts
    }

    def unescape(s:String):String = {
        var found = false
        for( i <- 0 until s.length ) {
            val ch = s.charAt(i)
            if( ch >= sep1 && ch <= sep5 ) found = true
        }
        if( !found ) return s

        var ts = ""
        for(ch <- s) {
            ch match {
                case c if c == sep4 =>
                    ts += " "
                case c if c == sep5 =>
                    ts += ":"
                case c =>
                    ts += c
            }
        }
        ts
    }

    def nameToArrayName(name:String,array:String):String = {
        if( array != "auto" ) return array
        if( name.toLowerCase.endsWith("_type") ) 
            return name.substring(0,name.length-5) + "_array_type" 
        name + "_array"
    }

}


