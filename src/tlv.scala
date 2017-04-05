package scalabpe.core

import java.io._
import java.nio.ByteBuffer
import java.net.InetAddress
import scala.collection.mutable.{HashMap,HashSet}
import scala.collection.mutable.ArrayBuffer
import scala.xml._

class TlvType( val name:String, val cls: Int, val code:Int, val fieldInfo:TlvFieldInfo) {

    val defaultValue = if( fieldInfo == null ) null else fieldInfo.defaultValue

    var itemType = TlvType.UNKNOWN

    var structNames = Array[String]()
    var structTypes = Array[Int]()
    var structLens = Array[Int]()
    var structFieldInfos = Array[TlvFieldInfo]()

    def hasFieldInfo() = structFieldInfos.exists( _ != null )

    // constructor for struct
    def this( name:String, cls: Int, code:Int, fieldInfo:TlvFieldInfo, 
        structNames : Array[String], structTypes : Array[Int], structLens : Array[Int], structFieldInfos : Array[TlvFieldInfo] 
        ) {
            this(name,cls,code,fieldInfo)
            this.structNames = structNames
            this.structTypes = structTypes
            this.structLens = structLens
            this.structFieldInfos = structFieldInfos
    }

    // constructor for array
    def this( name:String, cls: Int, code:Int, fieldInfo:TlvFieldInfo, itemType: TlvType) {
        this(name,cls,code,fieldInfo)
        this.itemType = itemType
    }

    override def toString() = {

        val s = new StringBuilder()

        s.append(name+","+cls+","+code)
        if( itemType != TlvType.UNKNOWN )
            s.append(","+itemType)
        else if(  structTypes != null )
            s.append(","+structNames.mkString("#")+","+structTypes.mkString("#")+","+structLens.mkString("#"))

        s.toString
    }
}

object TlvType {
    val UNKNOWN = new TlvType("unknown",TlvCodec.CLS_UNKNOWN,-1,null)
}

object TlvCodec {

    val CLS_ARRAY = -2
    val CLS_UNKNOWN = -1

    val CLS_STRING = 1
    val CLS_INT = 2
    val CLS_STRUCT = 3

    val CLS_STRINGARRAY = 4
    val CLS_INTARRAY = 5
    val CLS_STRUCTARRAY = 6

    val CLS_BYTES = 7

    val CLS_SYSTEMSTRING = 8 // used only in struct

    val EMPTY_STRINGMAP = new HashMapStringString()

    val MASK = 0xFF
    val HEX_CHARS = Array[Char]('0', '1', '2', '3', '4', '5', '6','7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

    def isArray(cls:Int): Boolean = { cls == CLS_STRINGARRAY || cls == CLS_INTARRAY || cls == CLS_STRUCTARRAY }

    def clsToArrayType(cls:Int) = {
        cls match {
            case CLS_STRING => CLS_STRINGARRAY
            case CLS_INT => CLS_INTARRAY
            case CLS_STRUCT => CLS_STRUCTARRAY
            case _ => CLS_UNKNOWN;
        }
    }

    def clsToInt(cls:String,isbytes:String = "") = {

        cls.toLowerCase match {
            case "string" => if( isbytes == "1" || isbytes == "true"  || isbytes == "TRUE" || isbytes == "y" || isbytes == "YES" ) CLS_BYTES else CLS_STRING
            case "int" => CLS_INT
            case "struct" => CLS_STRUCT
            case "array" => CLS_ARRAY
            case "systemstring" => CLS_SYSTEMSTRING
            case _ => CLS_UNKNOWN
        }

    }

    def bytes2hex(b:Array[Byte]):String = {
        val len = b.size
        val ss = new StringBuilder

        for ( i <- 0 until b.size ) {
            val t = b(i) & MASK
            val hi = t / 16
            val lo = t % 16
            ss.append( HEX_CHARS(hi) )
            ss.append( HEX_CHARS(lo) )
            ss.append(" ")
        }

        return ss.toString.trim
    }

    def toHexString(buff:ByteBuffer):String = {
        bytes2hex(buff.array)
    }

}


class TlvCodec(val configFile:String) extends Logging {

    val bufferSize = 1000
    val EMPTY_BUFFER = ByteBuffer.allocate(0)

    val typeCodeToNameMap = new HashMap[Int,TlvType]()
    val typeNameToCodeMap = new HashMap[String,TlvType]()

    val msgIdToNameMap = new HashMap[Int,String]()
    val msgNameToIdMap = new HashMap[String,Int]()
    val msgIdToOrigNameMap = new HashMap[Int,String]()

    val msgKeyToTypeMapForReq = new HashMap[Int,HashMapStringString]()
    val msgTypeToKeyMapForReq = new HashMap[Int,HashMapStringString]()
    val msgKeysForReq = new HashMap[Int,ArrayBufferString]()
    val msgKeyToFieldInfoMapForReq = new HashMap[Int,HashMap[String,TlvFieldInfo]]()

    val msgKeyToTypeMapForRes = new HashMap[Int,HashMapStringString]()
    val msgTypeToKeyMapForRes = new HashMap[Int,HashMapStringString]()
    val msgKeysForRes = new HashMap[Int,ArrayBufferString]()
    val msgKeyToFieldInfoMapForRes = new HashMap[Int,HashMap[String,TlvFieldInfo]]()

    val codecAttributes = new HashMapStringString()

    val msgAttributes = new HashMap[Int,HashMapStringString]()

    var serviceId:Int = _
    var serviceName:String = _
    var serviceOrigName:String = _
    var enableExtendTlv = false

    init

    def init(): Unit = {
        try {
            initInternal()
        }catch {
            case
            ex:Exception => log.error("tlv init failed, file={}, error={}",configFile,ex.getMessage);
            throw ex
        }
    }

    def allTlvTypes() : List[TlvType] = {
        typeNameToCodeMap.values.toList
    }
    def allMsgIds() : List[Int] = {
        msgKeyToTypeMapForReq.keys.toList
    }
    def findTlvType(code:Int) : TlvType = {
        typeCodeToNameMap.getOrElse(code,null)
    }

    def getAttribute(t:Node,field1:String):String = {
        var s =  (t \ ("@"+field1)).toString
        s
    }

    def getTlvFieldInfo(t:Node):TlvFieldInfo = {

        // 严格区分 null 和 空串
        val defaultValue = t.attribute("default") match {
            case Some(a) => a.toString
            case None => null
        }

        var validatorCls  = getAttribute(t,"validator")
        if( validatorCls == "" ) validatorCls = null
        val validatorParam  = getAttribute(t,"validatorParam")
        val returnCode  = getAttribute(t,"returnCode")
        var encoderCls  = getAttribute(t,"encoder")
        if( encoderCls == "" ) encoderCls = null
        val encoderParam  = getAttribute(t,"encoderParam")
        TlvFieldInfo.getTlvFieldInfo(defaultValue,validatorCls,validatorParam,returnCode,encoderCls,encoderParam)
    }

    def getTlvFieldInfo(t:Node,tlvType:TlvType):TlvFieldInfo = {

        val fieldInfo = getTlvFieldInfo(t)
        if( fieldInfo == null ) return tlvType.fieldInfo
        if( tlvType.fieldInfo == null ) return fieldInfo

        val defaultValue = if( fieldInfo.defaultValue != null ) fieldInfo.defaultValue else tlvType.fieldInfo.defaultValue

        var validatorCls:String  = null
        var validatorParam  = ""
        var returnCode  = ""

        if( fieldInfo.validator != null ) {
            validatorCls  = fieldInfo.validatorCls
            validatorParam  = fieldInfo.validatorParam
            returnCode  = fieldInfo.validatorReturnCode
        } else if( tlvType.fieldInfo.validator != null ) {
            validatorCls  = tlvType.fieldInfo.validatorCls
            validatorParam  = tlvType.fieldInfo.validatorParam
            returnCode  = tlvType.fieldInfo.validatorReturnCode
        }

        var encoderCls:String  = null
        var encoderParam  = ""

        if( fieldInfo.encoder != null ) {
            encoderCls  = fieldInfo.encoderCls
            encoderParam  = fieldInfo.encoderParam
        } else if( tlvType.fieldInfo.encoder != null ) {
            encoderCls  = tlvType.fieldInfo.encoderCls
            encoderParam  = tlvType.fieldInfo.encoderParam
        }

        TlvFieldInfo.getTlvFieldInfo(defaultValue,validatorCls,validatorParam,returnCode,encoderCls,encoderParam)
    }

    def validateAndEncode(a:Any,fieldInfo:TlvFieldInfo,needEncode:Boolean):Tuple2[Any,Int] = {
        if( fieldInfo == null ) return (a,0)
        var errorCode = 0
        var b = a
        if( fieldInfo.validator != null) {
            val ret = fieldInfo.validator.validate(a)
            if( ret != 0 ) {
                log.error("validate_value_error, value="+a+", ret="+ret)
                if( errorCode == 0 ) errorCode = ret
            }
        }
        if( needEncode && fieldInfo.encoder != null ) {
            b = fieldInfo.encoder.encode(a)
        }
        (b,errorCode)
    }

    def validateAndEncode(map:HashMapStringAny,tlvType:TlvType,needEncode:Boolean):Int = {

        var errorCode = 0
        var i = 0 
        while( i < tlvType.structNames.length ) {
            val fieldInfo = tlvType.structFieldInfos(i)
            if( fieldInfo != null ) {
                val key = tlvType.structNames(i)
                val value = map.getOrElse(key,null)
                if( fieldInfo.validator != null ) {
                    val ret = fieldInfo.validator.validate(value)
                    if( ret != 0 ) {
                        log.error("validate_map_error, key="+key+", value="+value+", ret="+ret)
                        if( errorCode == 0 ) errorCode = ret
                    }
                }
                if(  needEncode && fieldInfo.encoder != null ) {
                    val v = fieldInfo.encoder.encode(value)
                    map.put(key,v)
                }
            }
            i += 1
        }
        errorCode
    }

    def setStructDefault(map:HashMapStringAny,tlvType:TlvType):Unit = {

        var i = 0 
        while( i < tlvType.structNames.length ) {
            val fieldInfo = tlvType.structFieldInfos(i)
            if( fieldInfo != null ) {
                val key = tlvType.structNames(i)
                val value = map.getOrElse(key,null)
                if( value == null && fieldInfo.defaultValue != null ) {
                   map.put(key,fieldInfo.defaultValue)
                }
            }
            i += 1
        }
    }

    def validateAndEncode(keyMap:HashMapStringString,fieldInfoMap:HashMap[String,TlvFieldInfo],map:HashMap[String,Any],needEncode:Boolean):Int = {

        if( fieldInfoMap == null ) return 0

        var errorCode = 0

        for( (key,fieldInfo) <- fieldInfoMap ) {
            var tp = keyMap.getOrElse(key,null)
            val tlvType = typeNameToCodeMap.getOrElse(tp,null)
            var value = map.getOrElse(key,null)
            if( value == null && fieldInfo != null && fieldInfo.defaultValue != null ) {
                map.put(key,fieldInfo.defaultValue)
            } else if( value != null && tlvType.cls == TlvCodec.CLS_STRUCT && value.isInstanceOf[HashMapStringAny] ) {
                    val m = value.asInstanceOf[HashMapStringAny]
                    setStructDefault(m,tlvType)
            } else if( value != null && tlvType.cls == TlvCodec.CLS_STRUCTARRAY  && value.isInstanceOf[ArrayBufferMap] ) {
                val lm = value.asInstanceOf[ArrayBufferMap]
                for(m <- lm )
                    setStructDefault(m,tlvType)
            }
            value = map.getOrElse(key,null)
            if( value != null ) {
                tlvType.cls match {
                    case TlvCodec.CLS_INT => 
                        map.put(key,anyToInt(value))
                    case TlvCodec.CLS_STRING => 
                        map.put(key,anyToString(value))
                    case TlvCodec.CLS_STRUCT => 
                        map.put(key,anyToStruct(tlvType,value))
                    case TlvCodec.CLS_INTARRAY => 
                        map.put(key,anyToIntArray(value))
                    case TlvCodec.CLS_STRINGARRAY => 
                        map.put(key,anyToStringArray(value))
                    case TlvCodec.CLS_STRUCTARRAY => 
                        map.put(key,anyToStructArray(tlvType,value))
                    case _ => 
                }
            }

            value = map.getOrElse(key,null)
            if( value == null ) {
                val (v,ec) = validateAndEncode(null,fieldInfo,needEncode)
                if( ec != 0 && errorCode == 0 )  errorCode = ec
            } else {

                value match {
                    case i:Int =>
                        val (v,ec) = validateAndEncode(i,fieldInfo,needEncode)
                        if( ec != 0 && errorCode == 0 )  errorCode = ec
                        map.put(key,v)
                    case s:String =>
                        val (v,ec) = validateAndEncode(s,fieldInfo,needEncode)
                        if( ec != 0 && errorCode == 0 )  errorCode = ec
                        map.put(key,v)
                    case m:HashMapStringAny =>
                        val ec = validateAndEncode(m,tlvType,needEncode)
                        if( ec != 0 && errorCode == 0 )  errorCode = ec
                    case li:ArrayBufferInt =>
                        val (v,ec) = validateAndEncode(li,fieldInfo,needEncode)
                        if( ec != 0 && errorCode == 0 )  errorCode = ec
                        for( i <- 0 until li.size ) {
                            val (v,ec) = validateAndEncode(li(i),tlvType.itemType.fieldInfo,needEncode)
                            if( ec != 0 && errorCode == 0 )  errorCode = ec
                            li(i) = v.asInstanceOf[Int]
                        }
                    case ls:ArrayBufferString =>
                        val (v,ec) = validateAndEncode(ls,fieldInfo,needEncode)
                        if( ec != 0 && errorCode == 0 )  errorCode = ec
                        for( i <- 0 until ls.size ) {
                            val (v,ec) = validateAndEncode(ls(i),tlvType.itemType.fieldInfo,needEncode)
                            if( ec != 0 && errorCode == 0 )  errorCode = ec
                            ls(i) = v.asInstanceOf[String]
                        }
                    case lm:ArrayBufferMap =>
                        val (v,ec) = validateAndEncode(lm,fieldInfo,needEncode)
                        if( ec != 0 && errorCode == 0 )  errorCode = ec
                        for( i <- 0 until lm.size ) {
                            val ec = validateAndEncode(lm(i),tlvType.itemType,needEncode)
                            if( ec != 0 && errorCode == 0 )  errorCode = ec
                        }
                    case _ =>
                }

            }
        }

        errorCode
    }

    def initInternal(): Unit = {

        val cfgXml = XML.load(configFile)

        serviceId = (cfgXml \ "@id").toString.toInt
        serviceOrigName = (cfgXml \ "@name").toString
        serviceName = (cfgXml \ "@name").toString.toLowerCase
        val enableExtendTlvStr = (cfgXml \ "@enableExtendTlv").toString.toLowerCase
        enableExtendTlvStr match {
            case "1" | "y"  | "t" | "yes" | "true" => enableExtendTlv = true
            case _ => enableExtendTlv = false
        }
        val metadatas = cfgXml.attributes.filter(  _.key != "id").filter( _.key != "name").filter( _.key != "IsTreeStruct").filter( _.key != "enableExtendTlv")
        for( m <- metadatas ) {
            codecAttributes.put(m.key,m.value.text)
        }

        val codecSql = (cfgXml \ "sql").text.trim
        if( codecSql != "")
            codecAttributes.put("sql",codecSql)


        // if( codecAttributes.size > 0 ) println( codecAttributes )

        val types = cfgXml \ "type"

        for( t<-types ) {

            val name = (t \ "@name").toString.toLowerCase
            val cls = TlvCodec.clsToInt( (t \ "@class").toString, (t \ "@isbytes").toString )
            if( cls == TlvCodec.CLS_UNKNOWN || cls == TlvCodec.CLS_SYSTEMSTRING)
                throw new CodecException("class not valid, class=%s".format( (t \ "@class").toString))
            val code =  if( cls != TlvCodec.CLS_ARRAY )  (t \ "@code").toString.toInt else 0

            val metadatas = t.attributes.filter(  _.key != "code").filter( _.key != "name").filter( _.key != "class").filter( _.key != "isbytes")
            for( m <- metadatas ) {
                codecAttributes.put("type-"+name+"-"+m.key,m.value.text)
            }

            val fieldInfo = getTlvFieldInfo(t)

            cls match {

                case TlvCodec.CLS_ARRAY =>

                    val itemType = (t \ "@itemType").toString.toLowerCase
                    val itemConfig = typeNameToCodeMap.getOrElse(itemType,TlvType.UNKNOWN)
                    if( itemConfig == TlvType.UNKNOWN ) { // todo
                        throw new CodecException("itemType not valid,name=%s,itemType=%s".format(name,itemType ))
                    }
                    val arraycls = TlvCodec.clsToArrayType(itemConfig.cls)
                    val c = new TlvType(name,arraycls,itemConfig.code,fieldInfo,itemConfig)
                    if( arraycls == TlvCodec.CLS_STRUCTARRAY) {
                        c.structNames = itemConfig.structNames
                        c.structTypes = itemConfig.structTypes
                        c.structLens = itemConfig.structLens
                        c.structFieldInfos = itemConfig.structFieldInfos
                    }

                    if( typeCodeToNameMap.getOrElse(c.code,null) != null ) {
                        val tlvType = typeCodeToNameMap.getOrElse(c.code,null)
                        if( TlvCodec.isArray(tlvType.cls) )
                            throw new CodecException("multiple array definitions for one code!!! code=%d,serviceId=%d".format(c.code,serviceId))
                    }
                    typeCodeToNameMap.put(c.code,c); // overwrite itemtype's map

                    if( typeNameToCodeMap.getOrElse(c.name,null) != null ) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(c.name,serviceId))
                    }

                    typeNameToCodeMap.put(c.name,c);

                    val classex  = codecAttributes.getOrElse("classex-"+itemType,null)
                    if( classex != null ) {
                        codecAttributes.put("classex-"+name,classex)
                    }

                case TlvCodec.CLS_STRUCT =>

                    val structNames = new ArrayBufferString()
                    val structTypes = new ArrayBufferInt()
                    val structLens = new ArrayBufferInt()
                    val structFieldInfos = ArrayBuffer[TlvFieldInfo]()

                    val fields = t \ "field"

                    for( f <- fields ) {

                        val fieldName = (f \ "@name").toString
                        val fieldType = TlvCodec.clsToInt( (f \ "@type").toString )

                        if( fieldType != TlvCodec.CLS_INT && fieldType != TlvCodec.CLS_STRING && fieldType != TlvCodec.CLS_SYSTEMSTRING) {
                            throw new CodecException("not supported field type,name=%s,type=%s".format(name,fieldType))
                        }

                        var fieldLen = -1
                        var lenstr =  (f \ "@len").toString
                        fieldLen =  if( lenstr == "" ) -1 else lenstr.toInt

                        if( fieldType == TlvCodec.CLS_INT ) fieldLen = 4
                        if( fieldType == TlvCodec.CLS_SYSTEMSTRING ) fieldLen = 4 // special length

                        val fieldInfo = getTlvFieldInfo(f)

                        structNames += fieldName
                        structTypes += fieldType
                        structLens += fieldLen
                        structFieldInfos += fieldInfo

                        val classex = (f \ "@classex").toString
                        if( classex != "" ) {
                            codecAttributes.put("classex-"+name+"-"+fieldName,classex)
                            codecAttributes.put("classex-"+name,"some")
                        }

                        val itemmetadatas = f.attributes.filter(  _.key != "name").filter( _.key != "type").filter( _.key != "len")
                        for( m <- itemmetadatas ) {
                            codecAttributes.put("type-"+name+"-"+fieldName+"-"+m.key,m.value.text)
                        }
                    }

                    var lenOk = true
                    for( i <- 1 until structLens.length ) { // don't include the last field
                        if( structLens(i-1) == -1 ) {
                            lenOk = false
                        }
                    }

                    if( !lenOk ) {
                        throw new CodecException("struct length not valid,name=%s,serviceId=%d".format(name,serviceId))
                    }

                    val c = new TlvType(name,cls,code,fieldInfo, 
                        structNames.toArray ,structTypes.toArray,structLens.toArray,structFieldInfos.toArray
                        )

                    if( typeCodeToNameMap.getOrElse(c.code,null) != null ) {
                        throw new CodecException("code duplicated, code=%d,serviceId=%d".format(c.code,serviceId))
                    }

                    if( typeNameToCodeMap.getOrElse(c.name,null) != null ) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(c.name,serviceId))
                    }

                    typeCodeToNameMap.put(c.code,c);
                    typeNameToCodeMap.put(c.name,c);

                case _ =>

                    val c = new TlvType(name,cls,code,fieldInfo)

                    if( typeCodeToNameMap.getOrElse(c.code,null) != null ) {
                        throw new CodecException("code duplicated, code=%d,serviceId=%d".format(c.code,serviceId))
                    }

                    if( typeNameToCodeMap.getOrElse(c.name,null) != null ) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(c.name,serviceId))
                    }

                    typeCodeToNameMap.put(c.code,c);
                    typeNameToCodeMap.put(c.name,c);

                    val classex  = (t \ "@classex").toString
                    if( classex != "" ) {
                        codecAttributes.put("classex-"+name,classex)
                    }

            }

        }

        val messages = cfgXml \ "message"

        for( t<-messages) {

            val attributes = new HashMapStringString()

            val msgId = (t \ "@id").toString.toInt
            val msgNameOrig = (t \ "@name").toString
            val msgName = (t \ "@name").toString.toLowerCase

            if( msgIdToNameMap.getOrElse(msgId,null) != null ) {
                throw new CodecException("msgId duplicated, msgId=%d,serviceId=%d".format(msgId,serviceId))
            }

            if( msgNameToIdMap.getOrElse(msgName,null) != null ) {
                throw new CodecException("msgName duplicated, msgName=%s,serviceId=%d".format(msgName,serviceId))
            }

            msgIdToNameMap.put(msgId,msgName)
            msgNameToIdMap.put(msgName,msgId)
            msgIdToOrigNameMap.put(msgId,msgNameOrig)

            val metadatas = t.attributes.filter(  _.key != "id").filter( _.key != "name")
            for( m <- metadatas ) {
                attributes.put(m.key,m.value.text)
            }

            // dbbroker
            val sql = (t \ "sql" ).text.trim
            if(sql != "")
                attributes.put("sql",sql)

            val fieldsreq = t \ "requestParameter" \ "field"
            val m1req = new HashMapStringString()
            val m2req = new HashMapStringString()
            val m3req = new ArrayBufferString()

            val m4req = new HashMap[String,TlvFieldInfo]()

            for( f<-fieldsreq ) {
                val key = (f \ "@name").toString
                var typeName = (f \ "@type").toString.toLowerCase
                if( typeName == "" ) typeName = (key+"_type").toLowerCase

                val tlvType = typeNameToCodeMap.getOrElse(typeName,null)
                if( tlvType == null ) {
                    throw new CodecException("typeName %s not found".format(typeName));
                }

                if( m1req.getOrElse(key,null) != null ) {
                    throw new CodecException("req key duplicated, key=%s,serviceId=%d,msgId=%d".format(key,serviceId,msgId))
                }
                if( m2req.getOrElse(typeName,null) != null ) {
                    throw new CodecException("req type duplicated, type=%s,serviceId=%d,msgId=%d".format(typeName,serviceId,msgId))
                }


                m1req.put(key,typeName)
                m2req.put(typeName,key)
                m3req += key
                val fieldInfo = getTlvFieldInfo(f,tlvType)
                if( fieldInfo != null ) {
                    m4req.put(key,fieldInfo)
                } else if( tlvType.cls == TlvCodec.CLS_STRUCT ) {
                    if( tlvType.hasFieldInfo() ) 
                        m4req.put(key,null)
                } else if( tlvType.cls == TlvCodec.CLS_STRINGARRAY || tlvType.cls == TlvCodec.CLS_INTARRAY ) {
                    if( tlvType.itemType.fieldInfo != null ) 
                        m4req.put(key,null)
                } else if( tlvType.cls == TlvCodec.CLS_STRUCTARRAY ) {
                    if( tlvType.itemType.hasFieldInfo() ) 
                        m4req.put(key,null)
                }

                val metadatas = f.attributes.filter(  _.key != "name").filter( _.key != "type").filter( _.key != "required")
                for( m <- metadatas ) {
                    attributes.put("req-"+key+"-"+m.key,m.value.text)
                }

            }
            msgKeyToTypeMapForReq.put(msgId,m1req)
            msgTypeToKeyMapForReq.put(msgId,m2req)
            msgKeysForReq.put(msgId,m3req)
            if( m4req.size > 0 )
                msgKeyToFieldInfoMapForReq.put(msgId,m4req)

            val fieldsres = t \ "responseParameter" \ "field"
            val m1res = new HashMapStringString()
            val m2res = new HashMapStringString()
            val m3res = new ArrayBufferString()
            val m4res = new HashMap[String,TlvFieldInfo]()
            for( f<-fieldsres ) {
                val key = (f \ "@name").toString
                var typeName = (f \ "@type").toString.toLowerCase
                if( typeName == "" ) typeName = (key+"_type").toLowerCase

                val tlvType = typeNameToCodeMap.getOrElse(typeName,null)
                if( tlvType == null ) {
                    throw new CodecException("typeName %s not found".format(typeName));
                }

                if( m1res.getOrElse(key,null) != null ) {
                    throw new CodecException("res key duplicated, key=%s,serviceId=%d,msgId=%d".format(key,serviceId,msgId))
                }
                if( m2res.getOrElse(typeName,null) != null ) {
                    throw new CodecException("res type duplicated, type=%s,serviceId=%d,msgId=%d".format(typeName,serviceId,msgId))
                }

                m1res.put(key,typeName)
                m2res.put(typeName,key)
                m3res += key
                val fieldInfo = getTlvFieldInfo(f,tlvType)
                if( fieldInfo != null ) {
                    m4res.put(key,fieldInfo)
                } else if( tlvType.cls == TlvCodec.CLS_STRUCT ) {
                    if( tlvType.hasFieldInfo() ) 
                        m4res.put(key,null)
                } else if( tlvType.cls == TlvCodec.CLS_STRINGARRAY || tlvType.cls == TlvCodec.CLS_INTARRAY ) {
                    if( tlvType.itemType.fieldInfo != null ) 
                        m4res.put(key,null)
                } else if( tlvType.cls == TlvCodec.CLS_STRUCTARRAY ) {
                    if( tlvType.itemType.hasFieldInfo() ) 
                        m4res.put(key,null)
                }

                val metadatas = f.attributes.filter(  _.key != "name").filter( _.key != "type").filter( _.key != "required")
                for( m <- metadatas ) {
                    attributes.put("res-"+key+"-"+m.key,m.value.text)
                }
            }

            msgKeyToTypeMapForRes.put(msgId,m1res)
            msgTypeToKeyMapForRes.put(msgId,m2res)
            msgKeysForRes.put(msgId,m3res)
            if( m4res.size > 0 )
                msgKeyToFieldInfoMapForRes.put(msgId,m4res)

            msgAttributes.put(msgId,attributes)

            // if( attributes.size > 0 ) println( attributes )
        }

    }

    def decodeRequest(msgId:Int, buff:ByteBuffer,encoding:Int):Tuple2[HashMapStringAny,Int] = {
        try {
            val keyMap = msgTypeToKeyMapForReq.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val m = decode(keyMap,buff,encoding)
            val fieldInfoMap = msgKeyToFieldInfoMapForReq.getOrElse(msgId,null)
            val keyMap2 = msgKeyToTypeMapForReq.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val ec = validateAndEncode(keyMap2,fieldInfoMap,m,true)
            (m,ec)
        } catch {
            case e:Throwable =>
                log.error("decode request error",e)
                (HashMapStringAny(),ResultCodes.TLV_DECODE_ERROR)
        }
    } 

    def decodeResponse(msgId:Int, buff:ByteBuffer,encoding:Int):Tuple2[HashMapStringAny,Int] = {
        try {
            val keyMap = msgTypeToKeyMapForRes.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val m = decode(keyMap,buff,encoding)
            val fieldInfoMap = msgKeyToFieldInfoMapForRes.getOrElse(msgId,null)
            val keyMap2 = msgKeyToTypeMapForRes.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val ec = validateAndEncode(keyMap2,fieldInfoMap,m,false)
            (m,ec)
        } catch {
            case e:Throwable =>
                log.error("decode response error",e)
                (HashMapStringAny(),ResultCodes.TLV_DECODE_ERROR)
        }
    }

    def decode(keyMap:HashMapStringString, buff:ByteBuffer,encoding:Int):HashMapStringAny = {

        buff.position(0)
        val limit = buff.remaining

        val map = new HashMapStringAny()
        var break = false

        while (buff.position + 4 <= limit && !break ) {

            val code = buff.getShort.toInt;
            var len : Int = buff.getShort & 0xffff;
            var tlvheadlen = 4
            if( code > 0 && len == 0 ) { len = buff.getInt; tlvheadlen = 8; }

            if( len < tlvheadlen ) {
                if( code == 0 && len == 0 ) { // 00 03 00 04 00 00 00 00, the last 4 bytes are padding bytes by session server

                } else {
                    log.error("length_error,code="+code+",len="+len+",limit="+limit+",map="+map.toString);
                    if( log.isDebugEnabled ) {
                        log.debug("body bytes="+TlvCodec.toHexString(buff))
                    }
                }
                break = true
            }
            if( buff.position + len - tlvheadlen > limit ) {
                log.error("length_error,code="+code+",len="+len+",limit="+limit+",map="+map.toString);
                if( log.isDebugEnabled ) {
                    log.debug("body bytes="+TlvCodec.toHexString(buff))
                }
                break = true
            }

            if( !break ) {

                var config = typeCodeToNameMap.getOrElse(code,TlvType.UNKNOWN)

                var key = keyMap.getOrElse(config.name,null)

                // if is array, check to see if is itemType

                if( key == null && TlvCodec.isArray(config.cls) ) {
                    key = keyMap.getOrElse(config.itemType.name,null)
                    if( key != null ) {
                        config = config.itemType
                    }
                }

                if( config == TlvType.UNKNOWN || key == null ) {

                    var newposition = buff.position+aligned(len)-tlvheadlen
                    if( newposition > buff.limit) newposition = buff.limit

                    buff.position(newposition)

                } else {

                    config.cls match {

                        case TlvCodec.CLS_INT => {
                            if( len != 8 ) throw new CodecException("int_length_error,len="+len)
                            val value = buff.getInt
                        map.put(key,value)
                        }

                        case TlvCodec.CLS_STRING => {
                            val value = new String(buff.array(),buff.position,len-tlvheadlen,AvenueCodec.ENCODING_STRINGS(encoding))

                            map.put(key,value)

                            var newposition = buff.position+aligned(len)-tlvheadlen
                            if( newposition > buff.limit) newposition = buff.limit

                            buff.position(newposition)
                        }

                        case TlvCodec.CLS_BYTES => {
                            val p = buff.position
                            val value = new Array[Byte](len-tlvheadlen)
                            buff.get(value)
                            map.put(key,value)

                            var newposition = p+aligned(len)-tlvheadlen
                            if( newposition > buff.limit) newposition = buff.limit

                            buff.position(newposition)
                        }

                        case TlvCodec.CLS_STRUCT => {
                            val value = decodeStruct(buff,len-tlvheadlen,config,encoding)
                            map.put(key,value)
                        }

                        case TlvCodec.CLS_INTARRAY => {
                            if( len != 8 ) throw new CodecException("int_length_error,len="+len)
                            val value = buff.getInt

                        var a = map.getOrElse(key,null)
                        if( a == null ) {
                            val aa = new ArrayBufferInt()
                            aa += value
                            map.put(key,aa)
                        } else {
                            val aa = a.asInstanceOf[ArrayBufferInt]
                            aa += value
                        }
                        }

                        case TlvCodec.CLS_STRINGARRAY => {
                            val value = new String(buff.array(),buff.position,len-tlvheadlen,AvenueCodec.ENCODING_STRINGS(encoding))

                            var a = map.getOrElse(key,null)
                            if( a == null ) {
                                val aa = new ArrayBufferString()
                                aa += value
                                map.put(key,aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferString]
                                aa += value
                            }

                            var newposition = buff.position+aligned(len)-tlvheadlen
                            if( newposition > buff.limit) newposition = buff.limit

                            buff.position(newposition)
                        }

                        case TlvCodec.CLS_STRUCTARRAY => {

                            val value = decodeStruct(buff,len-tlvheadlen,config,encoding)

                            var a = map.getOrElse(key,null)
                            if( a == null ) {
                                val aa = new ArrayBufferMap()
                                aa += value
                                map.put(key,aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferMap]
                                aa += value
                            }
                        }

                        case _ => {

                            var newposition = buff.position+aligned(len)-tlvheadlen
                            if( newposition > buff.limit) newposition = buff.limit

                            buff.position(newposition)
                        }
                    }
                }
            }
        }


        map
    }

    def decodeStruct(buff:ByteBuffer,maxLen:Int,config:TlvType,encoding:Int): HashMapStringAny = {

        val map = new HashMapStringAny()

        var totalLen = 0
        for( i <- 0 until config.structNames.length ) {

            val key = config.structNames(i)
            val t = config.structTypes(i)
            var len = config.structLens(i)

            if( len == -1 ) len = maxLen - totalLen; // last field

            totalLen += len
            if( totalLen > maxLen ) {
                throw new CodecException("struct_data_not_valid")
            }

            t match {

                case TlvCodec.CLS_INT => {
                    val value = buff.getInt
                    map.put(key,value)
                }

                case TlvCodec.CLS_STRING => {
                    val value = new String(buff.array(),buff.position,len,AvenueCodec.ENCODING_STRINGS(encoding)).trim()
                    map.put(key,value)

                    var newposition = buff.position+aligned(len)
                    if( newposition > buff.limit) newposition = buff.limit

                    buff.position(newposition)
                }

                case TlvCodec.CLS_SYSTEMSTRING => {
                    len = buff.getInt  // length for system string
                    totalLen += aligned(len)
                    if( totalLen > maxLen ) {
                        throw new CodecException("struct_data_not_valid")
                    }
                    val value = new String(buff.array(),buff.position,len,AvenueCodec.ENCODING_STRINGS(encoding)).trim()
                    map.put(key,value)

                    var newposition = buff.position+aligned(len)
                    if( newposition > buff.limit) newposition = buff.limit

                    buff.position(newposition)
                }

                case _ =>

                    log.error("unknown type")
            }
        }

        map
    }

    def encodeRequest(msgId:Int, map:HashMap[String,Any],encoding:Int):Tuple2[ByteBuffer,Int] = {
        try {
            val keyMap = msgKeyToTypeMapForReq.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val fieldInfoMap = msgKeyToFieldInfoMapForReq.getOrElse(msgId,null)
            val ec = validateAndEncode(keyMap,fieldInfoMap,map,false)
            val b = encode(keyMap,map,encoding)
            (b,ec)
        } catch {
            case e:Throwable =>
                log.error("encode request error",e)
                (EMPTY_BUFFER,ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def encodeResponse(msgId:Int, map:HashMap[String,Any],encoding:Int):Tuple2[ByteBuffer,Int] = {
        try {
            val keyMap = msgKeyToTypeMapForRes.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val fieldInfoMap = msgKeyToFieldInfoMapForRes.getOrElse(msgId,null)
            val ec = validateAndEncode(keyMap,fieldInfoMap,map,true)
            val b = encode(keyMap,map,encoding)
            (b,ec)
        } catch {
            case e:Throwable =>
                log.error("encode response error",e)
                (EMPTY_BUFFER,ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def encode(keyMap:HashMapStringString, map:HashMap[String,Any],encoding:Int):ByteBuffer = {

        val buffs = new ArrayBuffer[ByteBuffer](1)
        buffs += ByteBuffer.allocate(bufferSize)

        for( (key,value) <- map
             if value != null ) {

                 val name = keyMap.getOrElse(key,null)
                 if( name != null ) {

                     val config = typeNameToCodeMap.getOrElse(name,TlvType.UNKNOWN)

                     config.cls match {

                         case TlvCodec.CLS_INT => {
                             encodeInt(buffs, config.code, value)
                         }

                         case TlvCodec.CLS_STRING => {
                             encodeString(buffs, config.code, value, encoding)
                         }

                         case TlvCodec.CLS_BYTES => {
                             encodeBytes(buffs, config.code, value)
                         }

                         case TlvCodec.CLS_STRUCT => {
                             encodeStruct(buffs, config, value, encoding)
                         }

                         case TlvCodec.CLS_INTARRAY => {
                             encodeIntArray(buffs, config.code, value)
                         }

                         case TlvCodec.CLS_STRINGARRAY => {
                             encodeStringArray(buffs, config.code, value, encoding)
                         }

                         case TlvCodec.CLS_STRUCTARRAY => {
                             encodeStructArray(buffs, config, value, encoding)
                         }

                         case _ => {
                             ;
                         }
                     }
                 }
        }
        buffs.foreach( _.flip() )

        if( buffs.size == 1 )
            return buffs(0)

        val total = buffs.foldLeft(0) { (sum,buff) => sum + buff.limit }
        val totalBuff = ByteBuffer.allocate(total)
        buffs.foreach( totalBuff.put )
        totalBuff.flip()
        buffs.clear
        totalBuff
    }

    def encodeInt(buffs: ArrayBuffer[ByteBuffer],code:Int, v:Any):Unit ={
        val value = anyToInt(v)
        val buff = findBuff(buffs,8)
        buff.putShort(code.toShort)
        buff.putShort(8.toShort)
        buff.putInt(value)
    }

    def encodeString(buffs: ArrayBuffer[ByteBuffer],code:Int, v:Any, encoding:Int):Unit= {
        val value = anyToString(v)
        if( value == null ) return
        val bytes = value.getBytes(AvenueCodec.ENCODING_STRINGS(encoding))
        var tlvheadlen = 4
        var alignedLen = aligned(bytes.length+tlvheadlen)
        if( alignedLen > 65535 && enableExtendTlv ) {
            tlvheadlen = 8
            alignedLen = aligned(bytes.length+tlvheadlen)
        }

        val buff = findBuff(buffs,alignedLen)
        buff.putShort(code.toShort)

        if( tlvheadlen == 8 ) {
            buff.putShort(0)
            buff.putInt(bytes.length+tlvheadlen)
        } else {
            buff.putShort((bytes.length+tlvheadlen).toShort)
        }

        buff.put(bytes)
        buff.position(buff.position + alignedLen - bytes.length - tlvheadlen )
    }

    def encodeBytes(buffs: ArrayBuffer[ByteBuffer], code:Int, v:Any ):Unit= {
        val bytes = v.asInstanceOf[Array[Byte]]
        var tlvheadlen = 4
        var alignedLen = aligned(bytes.length+tlvheadlen)
        if( alignedLen > 65535 && enableExtendTlv ) {
            tlvheadlen = 8
            alignedLen = aligned(bytes.length+tlvheadlen)
        }

        val buff = findBuff(buffs,alignedLen)
        buff.putShort(code.toShort)

        if( tlvheadlen == 8 ) {
            buff.putShort(0)
            buff.putInt(bytes.length+tlvheadlen)
        } else {
            buff.putShort((bytes.length+tlvheadlen).toShort)
        }

        buff.put(bytes)
        buff.position(buff.position + alignedLen - bytes.length - tlvheadlen )
    }

    def encodeStruct(buffs: ArrayBuffer[ByteBuffer],config:TlvType, value:Any, encoding:Int):Unit ={

        value match {

            case map: scala.collection.Map[_,_]=>

                val datamap = map.asInstanceOf[scala.collection.Map[String,Any]]
                val data = new ArrayBufferAny()
                var totalLen = 0

                for( i <- 0 until config.structNames.length ) {

                    val key = config.structNames(i)
                    val t = config.structTypes(i)
                    val len = config.structLens(i)

                    if( !datamap.contains(key) ) {
                        val allKeys = config.structNames
                        val missedKeys = allKeys.filter(!datamap.contains(_))
                        throw new CodecException("struct_not_valid, struct names= "+allKeys.mkString(",")+", missed keys="+missedKeys.mkString(","));
                    }

                    var v = datamap.getOrElse(key,"")

                    t match {

                        case TlvCodec.CLS_INT => {
                            totalLen += 4
                            data += anyToInt(v)
                        }

                        case TlvCodec.CLS_STRING => {

                            if(v == null) v = ""
                            val s = anyToString(v).getBytes(AvenueCodec.ENCODING_STRINGS(encoding))

                        var actuallen = s.length
                        if( len == -1 ||  s.length == len) {
                            totalLen += s.length
                            data += s
                        } else if( s.length < len ) {
                            totalLen += len
                            data += s
                            data += new Array[Byte](len-s.length) // pad zeros
                        } else {
                            throw new CodecException("string_too_long");
                        }
                        var alignedLen = aligned(len)
                        if( alignedLen != len) {
                            totalLen += (alignedLen - len) 
                            data += new Array[Byte](alignedLen-len) // pad zeros
                        }
                        }
                        case TlvCodec.CLS_SYSTEMSTRING => {

                            if(v == null) v = ""
                            val s = anyToString(v).getBytes(AvenueCodec.ENCODING_STRINGS(encoding))

                        var alignedLen = aligned(s.length)
                        totalLen += 4
                        data += anyToInt(s.length)
                        totalLen += alignedLen
                        data += s
                        if(  s.length != alignedLen) {
                            data += new Array[Byte](alignedLen-s.length) // pad zeros
                        } 
                        }

                        case _ =>

                            log.error("unknown type")

                    }

                }

                var tlvheadlen = 4
                var alignedLen = aligned(totalLen+tlvheadlen)

                if( alignedLen > 65535 && enableExtendTlv ) {
                    tlvheadlen = 8
                    alignedLen = aligned(totalLen+tlvheadlen)
                }

                val buff = findBuff(buffs,alignedLen)
                buff.putShort(config.code.toShort)

                if( tlvheadlen == 8 ) {
                    buff.putShort(0)
                    buff.putInt(totalLen+tlvheadlen)
                } else {
                    buff.putShort((totalLen+tlvheadlen).toShort)
                }

                for( v <- data ) {
                    v match {
                        case s:Array[Byte] =>
                            buff.put(s)
                        case i:Int =>
                            buff.putInt(i)
                        case _ =>

                    }
                }

                buff.position(buff.position + alignedLen - totalLen - tlvheadlen)

            case _ =>
                log.error("not supported type, type={}",value.getClass.getName)
        }
    }

    def encodeIntArray(buffs: ArrayBuffer[ByteBuffer],code:Int, value:Any):Unit ={

        value match {

            case list: Array[_] => list.foreach( encodeInt(buffs,code,_) )
            case list: List[_]  => list.foreach( encodeInt(buffs,code,_) )
            case list: ArrayBuffer[_]  => list.foreach( encodeInt(buffs,code,_) )
            case list: Seq[_]  => list.foreach( encodeInt(buffs,code,_) )
            case _ => log.error("not supported type, type={}",value.getClass.getName)
        }

    }

    def encodeStringArray(buffs: ArrayBuffer[ByteBuffer],code:Int, value:Any, encoding:Int):Unit= {

        value match {

            case list: Array[_] => list.foreach( a => encodeString(buffs,code, if( a == null) "" else a ,encoding) )
            case list: List[_]  => list.foreach( a => encodeString(buffs,code,if( a == null) "" else a,encoding) )
            case list: ArrayBuffer[_]  => list.foreach( a => encodeString(buffs,code,if( a == null) "" else a,encoding) )
            case list: Seq[_]  => list.foreach( a => encodeString(buffs,code,if( a == null) "" else a,encoding) )
            case _ => log.error("not supported type, type={}",value.getClass.getName)
        }
    }

    def encodeStructArray(buffs: ArrayBuffer[ByteBuffer],config:TlvType, value:Any, encoding:Int):Unit= {

        value match {

            case list: Array[_] => list.foreach( encodeStruct(buffs,config,_,encoding) )
            case list: List[_]  => list.foreach( encodeStruct(buffs,config,_,encoding) )
            case list: ArrayBuffer[_]  => list.foreach( encodeStruct(buffs,config,_,encoding) )
            case list: Seq[_]  => list.foreach( encodeStruct(buffs,config,_,encoding) )
            case _ => log.error("not supported type, type={}",value.getClass.getName)
        }

    }

    def encodeRequestInJvm(msgId:Int, map:HashMap[String,Any],needEncode:Boolean): Tuple2[HashMapStringAny,Int] = {
        try {
            val keyMap = msgKeyToTypeMapForReq.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val fieldInfoMap = msgKeyToFieldInfoMapForReq.getOrElse(msgId,null)
            val ec = validateAndEncode(keyMap,fieldInfoMap,map,needEncode)
            val m = filter(keyMap,map)
            (m,ec)
        } catch {
            case e:Throwable =>
                log.error("encode request in jvm error",e)
                (HashMapStringAny(),ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def encodeResponseInJvm(msgId:Int, map:HashMap[String,Any]): Tuple2[HashMapStringAny,Int] = {
        try {
            val keyMap = msgKeyToTypeMapForRes.getOrElse(msgId,TlvCodec.EMPTY_STRINGMAP)
            val fieldInfoMap = msgKeyToFieldInfoMapForRes.getOrElse(msgId,null)
            val ec = validateAndEncode(keyMap,fieldInfoMap,map,true)
            val m = filter(keyMap,map)
            (m,ec)
        } catch {
            case e:Throwable =>
                log.error("encode response in jvm error",e)
                (HashMapStringAny(),ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def filter(keyMap:HashMapStringString, map:HashMap[String,Any]): HashMapStringAny = {

        val retmap = new HashMapStringAny();

        for( (key,value) <- map if value != null) {
            val typeName = keyMap.getOrElse(key,null)
            if( typeName != null ) {

                val config = typeNameToCodeMap.getOrElse(typeName,TlvType.UNKNOWN)
                config.cls match {

                    case TlvCodec.CLS_INT => {
                        retmap.put(key,anyToInt(value))
                    }

                    case TlvCodec.CLS_STRING => {
                        retmap.put(key,anyToString(value))
                    }

                    case TlvCodec.CLS_BYTES => {
                        retmap.put(key,value)
                    }

                    case TlvCodec.CLS_STRUCT => {
                        val mm = anyToStruct(config,value)
                        if( mm != null )
                            retmap.put(key,mm)
                    }

                    case TlvCodec.CLS_INTARRAY => {
                        val mm = anyToIntArray(value)
                        if( mm != null )
                            retmap.put(key,mm)
                    }

                    case TlvCodec.CLS_STRINGARRAY => {
                        val mm = anyToStringArray(value)
                        if( mm != null )
                            retmap.put(key,mm)
                    }

                    case TlvCodec.CLS_STRUCTARRAY => {
                        val mm = anyToStructArray(config,value)
                        if( mm != null )
                            retmap.put(key,mm)
                    }

                    case _ => {
                        ;
                    }
                }
            }
        }

        retmap
    }

    def anyToInt(value:Any):Int = {
        TypeSafe.anyToInt(value)
    }
    def anyToString(value:Any):String ={
        TypeSafe.anyToString(value)
    }

    def anyToStruct(config:TlvType, value:Any): HashMapStringAny = {

        value match {

            case map: scala.collection.Map[_,_] =>

                val retmap = new HashMapStringAny()
                val datamap = map.asInstanceOf[scala.collection.Map[String,Any]]
                var i = 0
                while( i < config.structNames.length ) {

                    val key = config.structNames(i)
                    val t = config.structTypes(i)
                    val fieldInfo = config.structFieldInfos(i)

                    if( !datamap.contains(key) && (fieldInfo == null || fieldInfo.defaultValue == null ) ) {
                        val allKeys = config.structNames
                        val missedKeys = allKeys.filter(!datamap.contains(_))
                        throw new CodecException("struct_not_valid, struct names= "+allKeys.mkString(",")+", missed keys="+missedKeys.mkString(","));
                    }

                    var v = datamap.getOrElse(key,null)

                    if( v == null && fieldInfo != null && fieldInfo.defaultValue != null ) {
                        v = fieldInfo.defaultValue 
                    }
                    if( v == null ) v = ""

                    t match {

                        case TlvCodec.CLS_INT =>
                            retmap.put(key,anyToInt(v))

                        case TlvCodec.CLS_STRING =>
                            retmap.put(key,anyToString(v))

                        case TlvCodec.CLS_SYSTEMSTRING =>
                            retmap.put(key,anyToString(v))

                    }

                    i += 1
                }
                retmap
            case _ =>
                throw new CodecException("struct_not_valid");
        }
    }

    def anyToIntArray(value:Any):ArrayBufferInt = {

        val arr = new ArrayBufferInt()

        value match {

            case list: ArrayBufferInt =>
                list
            case list: ArrayBufferString =>
                list.foreach(arr += anyToInt(_) )
                arr
            case list: Array[_] =>
                list.foreach(arr += anyToInt(_))
                arr
            case list: List[_]  =>
                list.foreach(arr += anyToInt(_))
                arr
            case list: ArrayBuffer[_]  =>
                list.foreach(arr += anyToInt(_))
                arr
            case list: Seq[_]  =>
                list.foreach(arr += anyToInt(_))
                arr
            case _ =>
                log.error("not supported type, type={}",value.getClass.getName)
                null
        }
    }

    def anyToStringArray(value:Any):ArrayBufferString = {

        val arr = new ArrayBufferString()

        value match {

            case list: ArrayBufferString =>
                list
            case list: ArrayBufferInt =>
                list.foreach(arr += _.toString )
                arr
            case list: Array[_] =>
                list.foreach(arr += anyToString(_))
                arr
            case list: List[_]  =>
                list.foreach(arr += anyToString(_))
                arr
            case list: ArrayBuffer[_]  =>
                list.foreach(arr += anyToString(_))
                arr
            case list: Seq[_]  =>
                list.foreach(arr += anyToString(_))
                arr
            case _ =>
                log.error("not supported type, type={}",value.getClass.getName)
                null
        }
    }

    def changeTypeForMap(config:TlvType, datamap:HashMapStringAny) {
        var i = 0
        while( i < config.structNames.length ) {

            val key = config.structNames(i)
            val t = config.structTypes(i)
            val fieldInfo = config.structFieldInfos(i)

            if( !datamap.contains(key) && (fieldInfo == null || fieldInfo.defaultValue == null ) ) {
                val allKeys = config.structNames
                val missedKeys = allKeys.filter(!datamap.contains(_))
                throw new CodecException("struct_not_valid, struct names= "+allKeys.mkString(",")+", missed keys="+missedKeys.mkString(","));
            }

            var v = datamap.getOrElse(key,null)
            if( v == null && fieldInfo != null && fieldInfo.defaultValue != null ) {
                v = fieldInfo.defaultValue 
                datamap.put(key,v)
            }
            if( v == null ) v = ""

            t match {
                case TlvCodec.CLS_INT =>
                    if( !v.isInstanceOf[Int] )
                        datamap.put(key,anyToInt(v))
                case TlvCodec.CLS_STRING =>
                    if( !v.isInstanceOf[String] )
                        datamap.put(key,anyToString(v))
                case TlvCodec.CLS_SYSTEMSTRING =>
                    if( !v.isInstanceOf[String] )
                        datamap.put(key,anyToString(v))
            }


            i += 1
        }
    }


    def anyToStructArray(config:TlvType,value:Any):ArrayBufferMap = {

        val arr = new ArrayBufferMap()

        value match {
            case list: ArrayBufferMap =>
                list.foreach( changeTypeForMap(config,_))
                list
            case list: Array[_] =>
                list.foreach(arr += anyToStruct(config,_))
                arr
            case list: List[_]  =>
                list.foreach(arr += anyToStruct(config,_))
                arr
            case list: ArrayBuffer[_]  =>
                list.foreach(arr += anyToStruct(config,_))
                arr
            case list: Seq[_]  =>
                list.foreach(arr += anyToStruct(config,_))
                arr
            case _ =>
                log.error("not supported type, type={}",value.getClass.getName)
                null
        }
    }

    def findBuff(buffs: ArrayBuffer[ByteBuffer],len:Int): ByteBuffer = {

        /*  

        // c++ requirment: array tlv must be contined, so always append to last buff 

        val avails = buffs.filter( _.remaining >= len )
        if( avails.size > 0 )
            return avails(0)

         */
        var buff = buffs(buffs.size-1)
        if( buff.remaining >= len ) return buff

        val needLen = if( len < bufferSize ) bufferSize else len;
        buff = ByteBuffer.allocate(needLen)
        buffs += buff
        buff
    }

    def aligned(len:Int):Int = {
        if( (len & 0x03) != 0)
            ((len >> 2) + 1) << 2
        else
            len
    }

    def msgIdToName(msgId:Int) : String = {
        msgIdToNameMap.getOrElse(msgId,null)
    }
    def msgNameToId(msgName:String) : Int = {
        msgNameToIdMap.getOrElse(msgName,0)
    }

}

class TlvCodecs(val dir:String) extends Logging {

    val codecs_id = new HashMap[Int,TlvCodec]()
    val codecs_names = new HashMap[String,TlvCodec]()

    init;

    def init() {

        val xmls = new File(dir).listFiles.map( _.getPath ).filter( name => name.endsWith(".xml") )
        val xmls_sub = new File(dir).listFiles.filter( _.isDirectory ).flatMap( _.listFiles ).map( _.getPath ).filter( name => name.endsWith(".xml") )

        val nameset = new HashSet[String]()
        for( f <- xmls ) {
            val file = new File(f)
            nameset.add(file.getName)
        }
        for( f <- xmls_sub ) {
            val file = new File(f)
            if( nameset.contains(file.getName) )
                throw new RuntimeException("avenue config filename duplicated, file="+f)
        }

        val allxmls = new ArrayBufferString()
        xmls.foreach( allxmls += _ )
        xmls_sub.foreach( allxmls += _ )

        for(f <- allxmls ) {

            //val in = new InputStreamReader(new FileInputStream(f),"UTF-8")
            //val cfgXml = XML.load(in)
            //in.close()

            try {
                val cfgXml = XML.load(f)

                val name = (cfgXml \ "@name").toString.toLowerCase
                val id = (cfgXml \ "@id").toString.toInt

                val codec = new TlvCodec(f)

                if( codecs_id.getOrElse(id,null) != null ) {
                    throw new RuntimeException("service id duplicated serviceId="+id)
                }
                if( codecs_names.getOrElse(name,null) != null ) {
                    throw new RuntimeException("service name duplicated name="+name)
                }

                codecs_id.put(id,codec)
                codecs_names.put(name,codec)
            } catch {
                case e:Throwable =>
                    log.error("load xml failed, f="+f)
                    throw e
            }
        }

        log.info("validator size="+Validator.cache.size)
        log.info("encoder size="+Encoder.cache.size)
        log.info("tlvfieldinfo size="+TlvFieldInfo.cache.size)
    }

    def findTlvCodec(serviceId:Int):TlvCodec = {
        codecs_id.getOrElse(serviceId,null)
    }

    def serviceNameToId(service:String):Tuple2[Int,Int] = {
        val ss = service.toLowerCase.split("\\.")
        if( ss.length != 2 ) {
            return (0,0)
        }
        val codec = codecs_names.getOrElse(ss(0),null)
        if( codec == null ) {
            return (0,0)
        }
        val msgId= codec.msgNameToId(ss(1))
        if( msgId == 0 ) {
            return (0,0)
        }

        (codec.serviceId,msgId)
    }

    def serviceIdToName(serviceId:Int,msgId:Int):Tuple2[String,String] = {
        val codec = findTlvCodec(serviceId)
        if( codec == null ) {
            return ("service"+serviceId.toString,"msg"+msgId.toString)
        }

        val msgName = codec.msgIdToName(msgId)
        if( msgName == null ) {

            return (codec.serviceName,"msg"+msgId.toString)
        }

        (codec.serviceName,msgName)
    }

    def allServiceIds(): List[Int] = {
        codecs_id.keys.toList
    }
}


object TlvCodec4Xhead extends Logging {

    val SPS_ID_0 = "00000000000000000000000000000000"

    def decode(serviceId:Int, buff:ByteBuffer):HashMapStringAny = {
        try {
            val m = decodeInternal(serviceId,buff)
            m
        } catch{
            case e:Exception =>
                log.error("xhead decode exception, e={}",e.getMessage)
                new HashMapStringAny()
        }
    }

    def decodeInternal(serviceId:Int, buff:ByteBuffer):HashMapStringAny = {

        buff.position(0)

        val limit = buff.remaining

        val map = new HashMapStringAny()

        val isServiceId3 = ( serviceId == 3)

        if( isServiceId3 ) {
            // only a "socId" field
            var len = 32
            if( limit < len ) len = limit
            val value = new String(buff.array(),0,len).trim()
        map.put(AvenueCodec.KEY_SOC_ID,value)
        return map
        }

        var break = false

        while (buff.position + 4 <= limit && !break) {

            val code = buff.getShort.toInt;
            val len : Int = buff.getShort & 0xffff;

            if( len < 4 ) {
                if( log.isDebugEnabled ) {
                    log.debug("xhead_length_error,code="+code+",len="+len+",map="+map+",limit="+limit);
                    log.debug("xhead bytes="+TlvCodec.toHexString(buff))
                }
                break = true
            }
            if( buff.position + len - 4 > limit ) {
                if( log.isDebugEnabled ) {
                    log.debug("xhead_length_error,code="+code+",len="+len+",map="+map+",limit="+limit);
                    log.debug("xhead bytes="+TlvCodec.toHexString(buff))
                }
                break = true
            }

            if( !break ) {
                code match {

                    case AvenueCodec.CODE_GS_INFO =>
                        decodeGsInfo(buff,len,AvenueCodec.KEY_GS_INFOS,map)
                    case AvenueCodec.CODE_SOC_ID =>
                        decodeString(buff,len,AvenueCodec.KEY_SOC_ID,map)
                    case AvenueCodec.CODE_ENDPOINT_ID =>
                        decodeString(buff,len,AvenueCodec.KEY_ENDPOINT_ID,map)
                    case AvenueCodec.CODE_UNIQUE_ID =>
                        decodeString(buff,len,AvenueCodec.KEY_UNIQUE_ID,map)
                    case AvenueCodec.CODE_APP_ID =>
                        decodeInt(buff,len,AvenueCodec.KEY_APP_ID,map)
                    case AvenueCodec.CODE_AREA_ID =>
                        decodeInt(buff,len,AvenueCodec.KEY_AREA_ID,map)
                    case AvenueCodec.CODE_GROUP_ID =>
                        decodeInt(buff,len,AvenueCodec.KEY_GROUP_ID,map)
                    case AvenueCodec.CODE_HOST_ID =>
                        decodeInt(buff,len,AvenueCodec.KEY_HOST_ID,map)
                    case AvenueCodec.CODE_SP_ID =>
                        decodeInt(buff,len,AvenueCodec.KEY_SP_ID,map)
                    case AvenueCodec.CODE_SPS_ID =>
                        decodeString(buff,len,AvenueCodec.KEY_SPS_ID,map)
                    case AvenueCodec.CODE_HTTP_TYPE =>
                        decodeInt(buff,len,AvenueCodec.KEY_HTTP_TYPE,map)
                    case AvenueCodec.CODE_LOG_ID =>
                        decodeString(buff,len,AvenueCodec.KEY_LOG_ID,map)
                    case _ =>
                        var newposition = buff.position+aligned(len)-4
                        if( newposition > buff.limit) newposition = buff.limit
                        buff.position(newposition)
                }
            }
        }

        var a = map.getOrElse(AvenueCodec.KEY_GS_INFOS,null)
        if( a != null ) {
            val aa = a.asInstanceOf[ArrayBufferString]
            val firstValue = aa(0)
            map.put(AvenueCodec.KEY_GS_INFO_FIRST,firstValue)
            val lastValue = aa(aa.length-1)
            map.put(AvenueCodec.KEY_GS_INFO_LAST,lastValue)
        }
        map
    }

    def decodeInt(buff:ByteBuffer,len:Int,key:String,map:HashMapStringAny):Unit = {
        if( len != 8 ) return
        val value = buff.getInt
        if( !map.contains(key) ) 
            map.put(key,value)
    }
    def decodeString(buff:ByteBuffer,len:Int,key:String,map:HashMapStringAny):Unit = {
        val value = new String(buff.array(),buff.position,len-4)
        if( !map.contains(key) ) 
            map.put(key,value)
        var newposition = buff.position+aligned(len)-4
        if( newposition > buff.limit) newposition = buff.limit
        buff.position(newposition)
    }
    def decodeGsInfo(buff:ByteBuffer,len:Int,key:String,map:HashMapStringAny):Unit = {
        if( len != 12 ) return
        val ips = new Array[Byte](4)
        buff.get(ips)
        val port = buff.getInt

        val ipstr = InetAddress.getByAddress(ips).getHostAddress()
        val value = ipstr + ":" + port

        var a = map.getOrElse(key,null)
        if( a == null ) {
            val aa = new ArrayBufferString()
            aa += value
            map.put(key,aa)
        } else {
            val aa = a.asInstanceOf[ArrayBufferString]
            aa += value
        }
    }

    def encode(serviceId:Int, map:HashMapStringAny):ByteBuffer = {
        try {
            val buff = encodeInternal(serviceId,map)
            buff
        } catch{
            case e:Exception =>
                log.error("xhead encode exception, e={}",e.getMessage)
                // throw new RuntimeException(e.getMessage)
                val buff = ByteBuffer.allocate(0)
                return buff
        }
    }

    def encodeInternal(serviceId:Int, map:HashMapStringAny):ByteBuffer = {

        val socId = map.getOrElse(AvenueCodec.KEY_SOC_ID,null)
        val gsInfos = map.getOrElse(AvenueCodec.KEY_GS_INFOS,null)
        val appId = map.getOrElse(AvenueCodec.KEY_APP_ID,null)
        val areaId = map.getOrElse(AvenueCodec.KEY_AREA_ID,null)
        val groupId = map.getOrElse(AvenueCodec.KEY_GROUP_ID,null)
        val hostId= map.getOrElse(AvenueCodec.KEY_HOST_ID,null)
        val spId = map.getOrElse(AvenueCodec.KEY_SP_ID,null)
        val uniqueId = map.getOrElse(AvenueCodec.KEY_UNIQUE_ID,null)
        val endpointId = map.getOrElse(AvenueCodec.KEY_ENDPOINT_ID,null)
        val spsId = map.getOrElse(AvenueCodec.KEY_SPS_ID,null)
        val httpType = map.getOrElse(AvenueCodec.KEY_HTTP_TYPE,null)
        val logId = map.getOrElse(AvenueCodec.KEY_LOG_ID,null)
        val isServiceId3 = ( serviceId == 3)

        if( isServiceId3 ) {
            if( socId != null ) {
                val buff = ByteBuffer.allocate(32)
                val bs = TypeSafe.anyToString(socId).getBytes()
                val len = if(bs.length > 32) 32 else bs.length;

                buff.put(bs,0,len)
                for( i <- 0 until 32 - len ) {
                    buff.put(0.toByte)
                }
                buff.flip()
                return buff
            } else {
                val buff = ByteBuffer.allocate(0)
                return buff
            }
        }

        val buff = ByteBuffer.allocate(240)

        if( gsInfos != null ) {
            gsInfos match {
                case infos:ArrayBufferString =>
                    for(info <- infos ) {
                        encodeAddr(buff,AvenueCodec.CODE_GS_INFO, info)
                    }
                case _ =>
                    throw new CodecException("unknown gsinfos")
            }
        }
        if( socId != null )
            encodeString(buff,AvenueCodec.CODE_SOC_ID,socId)
        if( endpointId != null )
            encodeString(buff,AvenueCodec.CODE_ENDPOINT_ID,endpointId)
        if( uniqueId != null )
            encodeString(buff,AvenueCodec.CODE_UNIQUE_ID,uniqueId)
        if( appId != null )
            encodeInt(buff,AvenueCodec.CODE_APP_ID,appId)
        if( areaId != null )
            encodeInt(buff,AvenueCodec.CODE_AREA_ID,areaId)
        if( groupId != null )
            encodeInt(buff,AvenueCodec.CODE_GROUP_ID,groupId)
        if( hostId != null )
            encodeInt(buff,AvenueCodec.CODE_HOST_ID,hostId)
        if( spId != null )
            encodeInt(buff,AvenueCodec.CODE_SP_ID,spId)
        if( spsId != null )
            encodeString(buff,AvenueCodec.CODE_SPS_ID,spsId)
        if( httpType != null )
            encodeInt(buff,AvenueCodec.CODE_HTTP_TYPE,httpType)
        if( logId != null )
            encodeString(buff,AvenueCodec.CODE_LOG_ID,logId)

        buff.flip()
        buff
    }

    def appendGsInfo(buff:ByteBuffer,gsInfo:String,insertSpsId:Boolean=false):ByteBuffer = {

        var newlen = aligned(buff.limit)+12

        if( insertSpsId ) 
            newlen += ( aligned(gsInfo.length) + 4 + aligned(SPS_ID_0.length)+4 )

        val newbuff = ByteBuffer.allocate(newlen)
        if( insertSpsId ) {
            newbuff.position(aligned(newbuff.position))
            encodeString(newbuff,AvenueCodec.CODE_SPS_ID,SPS_ID_0)
            newbuff.position(aligned(newbuff.position))
            encodeString(newbuff,AvenueCodec.CODE_SOC_ID,gsInfo)
        }

        newbuff.position(aligned(newbuff.position))
        newbuff.put(buff)
        newbuff.position(aligned(newbuff.position))
        encodeAddr(newbuff,AvenueCodec.CODE_GS_INFO,gsInfo)

        newbuff.flip
        newbuff
    }


    def encodeAddr(buff: ByteBuffer,code:Int, s:String):Unit ={

        if( buff.remaining < 12 )
            throw new CodecException("xhead is too long")

        val ss = s.split(":")
        val ipBytes = InetAddress.getByName(ss(0)).getAddress()

        buff.putShort(code.toShort)
        buff.putShort(12.toShort)
        buff.put(ipBytes)
        buff.putInt(ss(1).toInt)
    }

    def encodeInt(buff: ByteBuffer,code:Int, v:Any):Unit ={
        if( buff.remaining < 8 )
            throw new CodecException("xhead is too long")
        val value = TypeSafe.anyToInt(v)
        buff.putShort(code.toShort)
        buff.putShort(8.toShort)
        buff.putInt(value)
    }

    def encodeString(buff: ByteBuffer,code:Int, v:Any):Unit= {
        val value = TypeSafe.anyToString(v)
        if( value == null ) return
        val bytes = value.getBytes() // don't support chinese
        val alignedLen = aligned( bytes.length+4 )
        if( buff.remaining < alignedLen )
            throw new CodecException("xhead is too long")
        buff.putShort(code.toShort)
        buff.putShort((bytes.length+4).toShort)
        buff.put(bytes)
        buff.position(buff.position + alignedLen - bytes.length - 4)
    }

    def aligned(len:Int):Int = {

        //len

        if( (len & 0x03) != 0)
            ((len >> 2) + 1) << 2
        else
            len
    }
}

