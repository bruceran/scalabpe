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
    通过此工具生成常用的sql语句, 相关的服务描述文件
    
*/

object GenSqlTool {

    val indent = "    "
    val indent2 = indent + indent
    val indent3 = indent + indent + indent

    def help() {
        println(
"""
usage: scalabpe.GenSqlTool [options]
options:
    -h|--help                   帮助信息
    -f|--file filename          从参数文件中读取命令行参数, 可以同时使用参数文件和命令行参数，命令行优先
    
    -t|--table tablename        指定表名
    -g|--get [params]           生成返回单条记录的select语句,params格式为 s1,s2,s3#c1=v1,c2=v2,c3, select的字段可以用*表示表的所有字段，需连接数据库
                                params中#号前面为逗号分隔的查询列, 查询列可用空格或as来定义别名
                                params中#号后面为逗号分隔的条件列，条件列可用=指定默认值, 否则生成:xxx这种占位符
                                参数中若有空格，则整个参数串应使用双引号转义

    -q|--query [params]         生成返回多条记录的select语句,params格式为同get

    -i|--insert [params]        生成insert语句,params格式为 f1=v1,f2=v2,f3, 用*表示表的所有字段，需连接数据库
                                params为用逗号分隔的更新列, 更新列可用=指定默认值,否则生成:xxx这种占位符

    -u|--update [params]        生成update语句,params格式为 f1=v1,f2=v2,f3#c1=v1,c2=v2,c3, 用*表示表的所有字段，需连接数据库
                                params参数中#号前面为逗号分隔的更新列, 可用=指定默认值,否则生成:xxx这种占位符
                                params参数中#号后面为逗号分隔的条件列, 可用=指定默认值,否则生成:xxx这种占位符

    -d|--delete [params]        生成delete语句,params格式为 c1=v1,c2=v2,c3
                                params为用逗号隔开的条件列, 条件列可用=指定默认值,否则生成:xxx这种占位符

       --rowcount               生成rowcount
       --rowCount               生成rowCount
       --table_in_msg name      生成的消息名中用到的表名
       --start_code             起始code, 如果不指定，则从1开始
       --start_messageid        起始消息id, 如果不指定，则从1开始

       --url connect_url        连接串, 可选，可根据该连接串自动获得table所有字段名
       --username username      连接用户
       --password password      连接密码
       --pk name                主键对应的字段名，默认为id

       --sdf path_to_file       指定服务描述文件名 
       --update_sdf             直接将结果写入sdf参数指定的服务描述文件中, 默认是输出到控制台

""")
   }

    var rc = "rowcount"
    var start_code = 1
    var start_messageid = 1
    var existed_types = HashMap[String,Tlv]()
    var existed_messagenames = HashSet[String]()

    class Tlv(var name:String,var cls:String="string") {
        var array = false
        val fields = ArrayBufferString()
    }

    def parseArgs(args:Array[String]):HashMapStringAny = {
        val map = HashMapStringAny()
        var i = 0
        val files = ArrayBufferString()
        while(i < args.size) {
            args(i) match {
                case "-h" | "--help" => 
                    return null
                case "-f" | "--file" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("file",v)
                    i += skip
                case "-t" | "--table" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("table",v)
                    i += skip
                case "--table_in_msg" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("table_in_msg",v)
                    i += skip
                case "-g" | "--get" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("get",v)
                    i += skip
                case "-q" | "--query" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("query",v)
                    i += skip
                case "-i" | "--insert" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("insert",v)
                    i += skip
                case "-u" | "--update" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("update",v)
                    i += skip
                case "-d" | "--delete" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    map.put("delete",v)
                    i += skip
                case "--start_code" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("start_code",v.toInt)
                    i += skip
                case "--start_messageid" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("start_messageid",v.toInt)
                    i += skip
                case "--rowCount" => 
                    map.put("rc","rowCount")
                    i += 1
                case "--rowcount" => 
                    map.put("rc","rowcount")
                    i += 1
                case "--url" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("url",v)
                    i += skip
                case "--username" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("username",v)
                    i += skip
                case "--password" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("password",v)
                    i += skip
                case "--pk" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("pk",v)
                    i += skip
                case "--sdf" => 
                    val (v,skip) = parseNextParam(args,i+1)
                    if( v == "" ) {
                        println("invalid option "+args(i))
                        return null
                    }
                    map.put("sdf",v)
                    i += skip
                case "--update_sdf" => 
                    map.put("update_sdf","1")
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

    def parseNextParam(args:Array[String],i:Int):Tuple2[String,Int] = {
        if( i >= args.size ) return ("",1)
        val s = args(i)
        if( s.startsWith("-")) return ("",1)
        return (s,2)
    }

    def readParamFile(file:String):Array[String] = {
        val s = readAllLines(file).mkString(" ").replaceAll(" +"," ")
        val ss = escape(s).split(" ")
        for( i <- 0 until ss.size ) {
            ss(i) = removeQuota(unescape(ss(i)))
        }
        ss
    }
 
    def removeQuota(s:String):String = {
        if( s.startsWith("\"") && s.endsWith("\"") ) {
            return s.substring(1,s.length-1)
        }
        s
    }
    def escape(s:String):String = {
        var inQuota = false
        var ts = ""
        for(c <- s) {
            if( inQuota ) {
                c match {
                    case ' ' =>
                        ts = ts + sep4
                    case '\"' =>
                        inQuota = false
                    case _ =>
                        ts = ts + c
                }
            } else if( c == '\"')  { 
                inQuota = true
            } else {
                ts = ts + c
            }
        }
        if( inQuota )
            ts = ts + '\"'
        ts
    }

    def unescape(s:String):String = {
        var ts = ""
        for(ch <- s) {
            ch match {
                case c if c == sep4 =>
                    ts += " "
                case c =>
                    ts += c
            }
        }
        ts
    }


    def main(args:Array[String]) {
        var params = parseArgs(args)
        if( params == null ) {
            help()
            return
        }

        var file = params.ns("file")
        if( file != "" ) {
            if( !new File(file).exists ) {
                println("invalid parameter file, file="+file)
                return 
            }
            val params_inpfile = readParamFile(file)
            var params2 = parseArgs(params_inpfile)
            if( params2 == null ) {
                help()
                return
            }
            params2 ++= params
            params = params2
        }

        if( params.size == 0 ) {
            help()
            return
        }

        rc = params.ns("rc","rowcount")
        val table = params.ns("table")
        val table_in_msg = parseKey(params.ns("table_in_msg",table).toLowerCase)
        var getFields = params.ns("get")
        var queryFields = params.ns("query")
        var insertFields = params.ns("insert")
        var updateFields = params.ns("update")
        var deleteFields = params.ns("delete")
        val pk = params.ns("pk","id")

        if( table == "" ) {
            help()
            return
        }

        var tableFields = ""
        if( params.ns("url") != "" ) {
            tableFields = getTableFields(params.ns("url"),params.ns("username"),params.ns("password"),table)
            println("table fields="+tableFields)
            if( tableFields == null || tableFields == "" ) {
                return
            }
            getFields = getFields.replace("*",tableFields)
            if( getFields == "" ) getFields = tableFields
            if( getFields.indexOf("#") < 0 ) getFields += "#"+pk

            queryFields = queryFields.replace("*",tableFields)
            if( queryFields == "" ) queryFields = tableFields

            insertFields = insertFields.replace("*",tableFields)
            if( insertFields == "" ) insertFields = tableFields

            val removePkFields = tableFields.split(",").filter(_!=pk).mkString(",")
            updateFields = updateFields.replace("*",removePkFields)
            if( updateFields == "" ) updateFields = removePkFields
            if( updateFields.indexOf("#") < 0 ) updateFields += "#"+pk

            if( deleteFields == "" ) deleteFields = pk
        }

        start_code = params.i("start_code",1)
        start_messageid = params.i("start_messageid",1)

        var sdf = params.ns("sdf")
        if( params.ns("update_sdf","0") == "1" && sdf == "" ) {
            println("sdf file not found")
            return
        }
        
        if( sdf != "" && !new File(sdf).exists() ) {
            val p1 = "avenue_conf"+File.separator+sdf
            if( new File(p1).exists ) {
                sdf = p1
            } else {
                println("not a valid sdf file, sdf="+sdf)
                return 
            }
        }
        if( sdf != "" ) {
            val (maxCode,maxMessageId,existTypes,existMessageNames) = loadService(sdf)
            if( maxCode == -1 ) {
                return
            }
            start_code = ( maxCode / 10 + 1 ) * 10
            start_messageid = ( maxMessageId / 10 + 1 ) * 10

            if( params.contains("start_code"))
                start_code = params.i("start_code",1)
            if( params.contains("start_messageid"))
                start_messageid = params.i("start_messageid",1)

            existed_types = existTypes
            existed_messagenames = existMessageNames
        }

        val tlvMap = new LinkedHashMap[String,Tlv]()
        tlvMap.put("rowcount_type",new Tlv("rowcount_type","int"))

        val buff = ArrayBufferString()

        if( params.contains("get") ) {
            params.put("is_query","false")
            buff ++= genSelect(table,tableFields,getFields,params,tlvMap)
        }
        if( params.contains("query") ) {
            params.put("is_query","true")
            buff ++= genSelect(table,tableFields,queryFields,params,tlvMap)
        }
        if( params.contains("insert") ) {
            buff ++= genInsert(table,tableFields,insertFields,params,tlvMap)
        }
        if( params.contains("update") ) {
            buff ++= genUpdate(table,tableFields,updateFields,params,tlvMap)
        }
        if( params.contains("delete") ) {
            buff ++= genDelete(table,tableFields,deleteFields,params,tlvMap)
        }

        val types_buff = genTypes(tlvMap)
        if( params.ns("update_sdf","0") == "0") {
            println()
            println(types_buff.mkString("\n"))
            println()
            println(buff.mkString("\n"))
        } else {
            val list0 = readAllLinesNoTrim(sdf)
            val newbuff = ArrayBufferString()

            var firstMessage = true
            for(l <- list0) {
                l match {
                    case l if l.trim.startsWith("<message") =>
                        if( firstMessage ) {
                            firstMessage = false
                            newbuff += ""
                            newbuff ++= types_buff
                            newbuff += ""
                        }
                    case l if l.trim.startsWith("</service") =>
                        newbuff += ""
                        newbuff ++= buff
                        newbuff += ""
                    case _ =>
                }
                newbuff += l
            }

            val s = newbuff.mkString("\n")
            FileUtils.writeStringToFile(new File(sdf),s,"UTF-8")
            println("file updated, sdf="+sdf)
        }
    }

    def genTypes(tlvMap:LinkedHashMap[String,Tlv]):ArrayBufferString = {
        val types_buff = ArrayBufferString()
        for( (k,tlv) <- tlvMap ) {
            var s = ""
            if( tlv.cls == "struct") {
                val exist = checkExist(tlv)
                if( !exist ) {
                    s = indent + """<type name="%s" class="%s" code="%d">""".format(tlv.name,tlv.cls,start_code)
                    start_code += 1
                    types_buff += s
                    for( f <- tlv.fields ) {
                        s = indent2 + """<field name="%s" type="systemstring"/>""".format(f)
                        types_buff += s
                    }
                    s = indent + """</type>"""
                    types_buff += s
                }
            } else {
                val exist = existed_types.contains(tlv.name)
                if( !exist ) {
                    s = indent + """<type name="%s" class="%s" code="%d"/>""".format(tlv.name,tlv.cls,start_code)
                    start_code += 1
                    types_buff += s
                }
            }
            if(tlv.array) {
                val arrayName = tlv.name.substring(0,tlv.name.length-5)+"_array_type"
                val exist = existed_types.contains(arrayName)
                if( !exist ) {
                    s = indent + """<type name="%s" class="array" itemType="%s"/>""".format(arrayName,tlv.name)
                    types_buff += s
                }
            }
        }
        types_buff
    }
    def checkExist(tlv:Tlv):Boolean = {
        if( !existed_types.contains(tlv.name.toLowerCase) ) return false
        val existedTlv = existed_types.getOrElse(tlv.name.toLowerCase,null)
        if( existedTlv.fields.mkString(",").toLowerCase == tlv.fields.mkString(",").toLowerCase ) {
            return true
        } 
        val basename = tlv.name.substring(0,tlv.name.length-5)
        val nextname = 
            if( !basename.matches(".*[1-9]$"))
                basename + "2"
            else
                basename.substring(0,basename.length-1)+(basename.substring(basename.length-1).toInt+1)
        tlv.name = nextname+"_type"
        checkExist(tlv)
    }

    def checkMessageNameExist(name:String):String = {
        if( !existed_messagenames.contains(name.toLowerCase) ) return name
        val nextname = 
            if( !name.matches(".*[1-9]$"))
                name + "2"
            else
                name.substring(0,name.length-1)+(name.substring(name.length-1).toInt+1)
        checkMessageNameExist(nextname)
    }

    def loadService(file:String) : Tuple4[Int,Int,HashMap[String,Tlv],HashSet[String]] = {

        try {
            XML.load(file)
        } catch {
            case e:Throwable =>
                println("xml cannot be loaded, message="+e.getMessage)
                return (-1,-1,null,null)
        }

        val list0 = readAllLines(file)
        val list1 = mergeXmlTags(list0,"<!--","-->",sep3)
        val list2 = mergeXmlTags(list1,"<sql>","</sql>",sep3)
        val list3 = mergeXmlTags(list2,"<",">",' ')

        var maxCode = 0
        var maxMessageId = 0
        var existTypes = HashMap[String,Tlv]()
        var existMessageNames = HashSet[String]()
        var context = ""
        var contextTlv:Tlv = null
        for( l <- list3 ) {

            l match {
                case l if l.startsWith("<type ") =>
                    val attrs = parseAllXmlAttrs(l)
                    var cls = attrs.ns("class")
                    if( cls != "array" ) {
                        var code = attrs.ns("code").toInt
                        if( code > maxCode ) maxCode = code
                        var name = attrs.ns("name").toLowerCase
                        val tlv = new Tlv(name,cls)
                        existTypes.put(name,tlv)
                        if( cls == "struct" ) {
                            context = "struct"
                            contextTlv = tlv
                        }
                    } else {
                        val itemType = attrs.ns("itemType").toLowerCase
                        val tlv = existTypes.getOrElse(itemType,null)
                        if( tlv != null ) tlv.array = true
                        var name = attrs.ns("name").toLowerCase
                        val arraytlv = new Tlv(name,cls)
                        existTypes.put(name,arraytlv)
                    }
                case l if l.startsWith("<field ") =>
                    if( context == "struct" ) {
                        val attrs = parseAllXmlAttrs(l)
                        var name = attrs.ns("name")
                        contextTlv.fields += name
                    }
                case l if l.startsWith("</type>") =>
                    context = ""
                    contextTlv = null
                case l if l.startsWith("<message ") =>
                    val attrs = parseAllXmlAttrs(l)
                    var id = attrs.ns("id").toInt
                    if( id > maxMessageId ) maxMessageId = id
                    var name = attrs.ns("name").toLowerCase
                    existMessageNames.add(name)
                case _ =>
            }
        }
        (maxCode,maxMessageId,existTypes,existMessageNames)
    }

    def getTableFields(url:String,username:String,password:String,table:String):String = {
        import java.sql._
        val sql = "select * from %s where 1=0".format(table)
        try {
            val c = DriverManager.getConnection(url,username,password)
            val stmt = c.createStatement()
            val rs = stmt.executeQuery(sql)
            val md = rs.getMetaData()
            var fields = ArrayBufferString()
            for(i <- 1 to md.getColumnCount())
                fields += md.getColumnName(i)
            fields.mkString(",")
        } catch {
            case e:Throwable =>
                println("execute sql, sql="+sql)
                println("db exception, e="+e.getMessage)
                ""
        }
    }

    def genSelect(table:String,tableFields:String,userFields:String,params:HashMapStringAny,tlvMap:LinkedHashMap[String,Tlv]):ArrayBufferString = {
        val isQuery = params.ns("is_query") == "true"
        val fields = if( userFields != "" ) userFields else tableFields 
        if( fields == "" ) {
            if( isQuery )
                println("query params not defined")
            else
                println("get params not defined")
            return ArrayBufferString()
        }

        val p = fields.indexOf("#")
        var outfields = ""
        var condfields = ""
        if( p >= 0 ) {
            outfields = fields.substring(0,p)
            condfields = fields.substring(p+1)
        } else {
            outfields = fields
        }
        val outMap = outFieldsToMap(outfields)
        val condMap = condFieldsToMap(condfields)

        val table_in_msg = params.ns("table_in_msg")

        val buff = ArrayBufferString()
        if( isQuery ) {
            var messagename = checkMessageNameExist("query_"+table_in_msg)
            buff += indent + "<message name=\"%s\" id=\"%d\">".format(messagename,start_messageid)
        } else {
            var messagename = checkMessageNameExist("get_"+table_in_msg)
            buff += indent + "<message name=\"%s\" id=\"%d\">".format(messagename,start_messageid)
        }
        start_messageid += 1

        buff += indent2 + "<sql><![CDATA[" 
        buff += indent3+ genSelect(table,outMap,condMap)
        buff += indent2 + "]]></sql>" 
        buff += indent2 + "<requestParameter>" 
        for( (k,v) <- condMap if v == "" ) {
            val tp = (parseKey(k) + "_type").toLowerCase
            tlvMap.put(tp,new Tlv(tp))
            buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(parseKey(k),tp)
        }
        buff += indent2 + "</requestParameter>" 
        buff += indent2 + "<responseParameter>" 
        buff += indent3 + "<field name=\"%s\" type=\"%s_type\"/>".format(rc,rc)
        if( isQuery ) {
            val name = table_in_msg+"info_array"
            val tp = (table_in_msg+"info_array_type").toLowerCase
            val itemTp = (table_in_msg+"info_type").toLowerCase
            val tlv = new Tlv(itemTp,"struct")
            tlv.array = true
            for( (k,v) <- outMap ) {
                tlv.fields += parseKey(v.toString)
            }
            tlvMap.put(itemTp,tlv)
            buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(name,tp)
        } else {
            for( (k,v) <- outMap ) {
                val tp = (parseKey(v.toString)+"_type").toLowerCase
                tlvMap.put(tp,new Tlv(tp))
                buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(parseKey(v.toString),tp)
            }
        }
        buff += indent2 + "</responseParameter>" 
        buff += indent + "</message>" 
        buff += ""

        buff
    }

    def genInsert(table:String,tableFields:String,userFields:String,params:HashMapStringAny,tlvMap:LinkedHashMap[String,Tlv]):ArrayBufferString = {
        val fields = if( userFields != "" ) userFields else tableFields 
        if( fields == "" ) {
            println("insert params not defined")
            return ArrayBufferString()
        }

        val condMap = condFieldsToMap(fields)
        val table_in_msg = params.ns("table_in_msg")

        val buff = ArrayBufferString()
        var messagename = checkMessageNameExist("insert_"+table_in_msg)
        buff += indent + "<message name=\"%s\" id=\"%d\">".format(messagename,start_messageid)
        start_messageid += 1

        buff += indent2 + "<sql><![CDATA[" 
        buff += indent3+ genInsert(table,condMap)
        buff += indent2 + "]]></sql>" 
        buff += indent2 + "<requestParameter>" 
        for( (k,v) <- condMap if v == "" ) {
            val tp = (parseKey(k) + "_type").toLowerCase
            tlvMap.put(tp.toLowerCase,new Tlv(tp))
            buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(parseKey(k),tp)
        }
        buff += indent2 + "</requestParameter>" 
        buff += indent2 + "<responseParameter>" 
            buff += indent3 + "<field name=\"%s\" type=\"%s_type\"/>".format(rc,rc)
        buff += indent2 + "</responseParameter>" 
        buff += indent + "</message>" 
        buff += ""

        buff
    }

    def genUpdate(table:String,tableFields:String,userFields:String,params:HashMapStringAny,tlvMap:LinkedHashMap[String,Tlv]):ArrayBufferString = {
        val fields = if( userFields != "" ) userFields else tableFields 
        if( fields == "" ) {
            println("update params not defined")
            return ArrayBufferString()
        }

        val p = fields.indexOf("#")
        var updatefields = ""
        var condfields = ""
        if( p >= 0 ) {
            updatefields = fields.substring(0,p)
            condfields = fields.substring(p+1)
        } else {
            updatefields = fields
        }
        val updateMap = condFieldsToMap(updatefields)
        val condMap = condFieldsToMap(condfields)
        val table_in_msg = params.ns("table_in_msg")

        val buff = ArrayBufferString()
        var messagename = checkMessageNameExist("update_"+table_in_msg)
        buff += indent + "<message name=\"%s\" id=\"%d\">".format(messagename,start_messageid)
        start_messageid += 1

        buff += indent2 + "<sql><![CDATA[" 
        buff += indent3+ genUpdate(table,updateMap,condMap)
        buff += indent2 + "]]></sql>" 
        buff += indent2 + "<requestParameter>" 
        for( (k,v) <- updateMap if v == "" ) {
            val tp = (parseKey(k) + "_type").toLowerCase
            tlvMap.put(tp.toLowerCase,new Tlv(tp))
            buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(parseKey(k),tp)
        }
        for( (k,v) <- condMap if v == "" && !updateMap.contains(k)) {
            val tp = (parseKey(k) + "_type").toLowerCase
            tlvMap.put(tp.toLowerCase,new Tlv(tp))
            buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(parseKey(k),tp)
        }
        buff += indent2 + "</requestParameter>" 
        buff += indent2 + "<responseParameter>" 
            buff += indent3 + "<field name=\"%s\" type=\"%s_type\"/>".format(rc,rc)
        buff += indent2 + "</responseParameter>" 
        buff += indent + "</message>" 
        buff += ""

        buff
    }

    def genDelete(table:String,tableFields:String,userFields:String,params:HashMapStringAny,tlvMap:LinkedHashMap[String,Tlv]):ArrayBufferString = {
        val fields = if( userFields != "" ) userFields else tableFields 
        if( fields == "" ) {
            println("delete params not defined")
            return ArrayBufferString()
        }

        val condMap = condFieldsToMap(fields)
        val table_in_msg = params.ns("table_in_msg")

        val buff = ArrayBufferString()
        var messagename = checkMessageNameExist("delete_"+table_in_msg)
        buff += indent + "<message name=\"%s\" id=\"%d\">".format(messagename,start_messageid)
        start_messageid += 1

        buff += indent2 + "<sql><![CDATA[" 
        buff += indent3+ genDelete(table,condMap)
        buff += indent2 + "]]></sql>" 
        buff += indent2 + "<requestParameter>" 
        for( (k,v) <- condMap if v == "" ) {
            val tp = (parseKey(k) + "_type").toLowerCase
            tlvMap.put(tp.toLowerCase,new Tlv(tp))
            buff += indent3 + "<field name=\"%s\" type=\"%s\"/>".format(parseKey(k),tp)
        }
        buff += indent2 + "</requestParameter>" 
        buff += indent2 + "<responseParameter>" 
            buff += indent3 + "<field name=\"%s\" type=\"%s_type\"/>".format(rc,rc)
        buff += indent2 + "</responseParameter>" 
        buff += indent + "</message>" 
        buff += ""

        buff
    }

    def genSelect(table:String,outMap:LinkedHashMapStringAny,condMap:LinkedHashMapStringAny):String = {
        var s = "select "+outMap.keys.mkString(",")+" from " + table
        if( condMap.size > 0 ) { 
            s +=  "\n"+indent3+"where "
            var first = true
            for( (k,v) <- condMap ) {
                if( first ) {
                    first = false
                } else {
                    s += ","
                }
                if(v=="")
                    s += " " + k + "=:" + parseKey(k)
                else
                    s += " " + k + "=" + v
            }
        }
        s
    }

    def genInsert(table:String,condMap:LinkedHashMapStringAny):String = {
        var keys = ""
        var values = ""
        if( condMap.size > 0 ) { 
            var first = true
            for( (k,v) <- condMap ) {
                if( first ) {
                    first = false
                } else {
                    keys += ","
                    values += ","
                }
                if(v=="") {
                    keys += k
                    values += ":"+parseKey(k)
                }
                else {
                    keys += k
                    values += v
                }
            }
        }
        var s = "insert into %s(%s)\n%svalues(%s)".format(table,keys,indent3,values)
        s
    }

    def genUpdate(table:String,updateMap:LinkedHashMapStringAny,condMap:LinkedHashMapStringAny):String = {
        var s = "update " + table
        if( updateMap.size > 0 ) { 
            s +=  " set "
            var first = true
            for( (k,v) <- updateMap ) {
                if( first ) {
                    first = false
                } else {
                    s += ","
                }
                if(v=="")
                    s += " " + k + "=:" + parseKey(k)
                else
                    s += " " + k + "=" + v
            }
        }
        if( condMap.size > 0 ) { 
            s +=  "\n"+indent3+"where "
            var first = true
            for( (k,v) <- condMap ) {
                if( first ) {
                    first = false
                } else {
                    s += ","
                }
                if(v=="")
                    s += " " + k + "=:" + parseKey(k)
                else
                    s += " " + k + "=" + v
            }
        }

        s
    }

    def genDelete(table:String,condMap:LinkedHashMapStringAny):String = {
        var s = "delete from " + table
        if( condMap.size > 0 ) { 
            s +=  "\n"+indent3+"where "
            var first = true
            for( (k,v) <- condMap ) {
                if( first ) {
                    first = false
                } else {
                    s += ","
                }
                if(v=="")
                    s += " " + k + "=:" + parseKey(k)
                else
                    s += " " + k + "=" + v
            }
        }
        s
    }

    def condFieldsToMap(s:String):LinkedHashMapStringAny = {
        val m = new LinkedHashMapStringAny()
        if( s.trim == "" ) return m
        val ss = s.split(",").map(_.trim)
        for( s <- ss ) {
            var key = s
            var value = ""
            val p = s.indexOf("=")
            if( p >= 0 ) {
                key = s.substring(0,p)
                value = s.substring(p+1)
            }
            m.put(key,value)
        }
        m
    }

    def outFieldsToMap(s:String):LinkedHashMapStringAny = {
        val m = new LinkedHashMapStringAny()
        val ss = s.split(",").map(_.trim)
        for( s <- ss ) {
            var alias = s
            val p = s.lastIndexOf(" ")
            if( p >= 0 ) alias = s.substring(p+1)
            m.put(s,alias)
        }
        m
    }

    def parseKey(s:String):String = {
        val p = s.lastIndexOf(".")
        if( p >= 0 ) s.substring(p+1)
        else s
    }

}

