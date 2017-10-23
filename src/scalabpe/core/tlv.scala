package scalabpe.core

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.xml.Node
import scala.xml.XML

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

object FieldInfo extends Logging {

    val cache = new HashMap[String, FieldInfo]()

    def getFieldInfo(defaultValue: String,
                     validatorCls: String, validatorParam: String, validatorReturnCode: String,
                     encoderCls: String, encoderParam: String): FieldInfo = {

        if (defaultValue == null && validatorCls == null && encoderCls == null) return null

        val key = "defaultValue=" + defaultValue + ",validatorCls=" + validatorCls + ",validatorParam=" + validatorParam + ",validatorReturnCode=" + validatorReturnCode + ",encoderCls=" + encoderCls + ",encoderParam=" + encoderParam
        val v0 = cache.getOrElse(key, null)
        if (v0 != null) return v0

        val v = new FieldInfo(defaultValue, validatorCls, validatorParam, validatorReturnCode, encoderCls, encoderParam)
        cache.put(key, v)
        return v
    }
}

class FieldInfo(val defaultValue: String,
                val validatorCls: String, val validatorParam: String, val validatorReturnCode: String,
                val encoderCls: String, val encoderParam: String) {
    val validator = Validator.getValidator(validatorCls, validatorParam, validatorReturnCode)
    val encoder = Encoder.getEncoder(encoderCls, encoderParam)
    override def toString(): String = {
        "defaultValue=%s,validatorCls=%s,encoderCls=%s".format(defaultValue, validatorCls, encoderCls)
    }
}

object TlvType {

    val CLS_ARRAY = -2
    val CLS_UNKNOWN = -1

    val CLS_STRING = 1
    val CLS_INT = 2
    val CLS_LONG = 3
    val CLS_DOUBLE = 4
    val CLS_STRUCT = 5
    val CLS_OBJECT = 6

    val CLS_STRINGARRAY = 11
    val CLS_INTARRAY = 12
    val CLS_LONGARRAY = 13
    val CLS_DOUBLEARRAY = 14
    val CLS_STRUCTARRAY = 15
    val CLS_OBJECTARRAY = 16

    val CLS_BYTES = 21
    val CLS_SYSTEMSTRING = 22 // used only in struct for avenue version 1
    val CLS_VSTRING = 23 

    val UNKNOWN = new TlvType("unknown", CLS_UNKNOWN, -1, null)
    val EMPTY_STRUCTDEF = new StructDef()
    val EMPTY_OBJECTDEF = new ObjectDef()

    def isSimple(cls: Int): Boolean = {
        cls >= 1 && cls <= 4
    }
    def isArray(cls: Int): Boolean = {
        cls >= 11 && cls <= 16
    }

    def clsToArrayType(cls: Int): Int = {
        if (cls >= 1 && cls <= 6) return 10 + cls
        else return CLS_UNKNOWN
    }

    def clsToInt(cls: String, isbytes: String = ""): Int = {
        cls.toLowerCase match {
            case "string"       => if (TypeSafe.isTrue(isbytes)) CLS_BYTES else CLS_STRING
            case "bytes"        => CLS_BYTES
            case "int"          => CLS_INT
            case "long"         => CLS_LONG
            case "double"       => CLS_DOUBLE
            case "struct"       => CLS_STRUCT
            case "object"       => CLS_OBJECT
            case "array"        => CLS_ARRAY
            case "systemstring" => CLS_SYSTEMSTRING
            case "vstring"      => CLS_VSTRING
            case _              => CLS_UNKNOWN
        }
    }

    def clsToName(cls: Int): String = {
        cls match {
            case CLS_STRING       => "string"
            case CLS_BYTES        => "bytes"
            case CLS_INT          => "int"
            case CLS_LONG         => "long"
            case CLS_DOUBLE       => "double"
            case CLS_STRUCT       => "struct"
            case CLS_OBJECT       => "object"
            case CLS_ARRAY        => "array"
            case CLS_SYSTEMSTRING => "systemstring"
            case CLS_VSTRING      => "vstring"
            case _                => "unknown cls"
        }
    }

    def checkTypeCls(tp: Int): Boolean = {
        tp match {
            case CLS_INT    => true
            case CLS_LONG   => true
            case CLS_DOUBLE => true
            case CLS_STRING => true
            case CLS_BYTES  => true
            case _          => false
        }
    }

    def checkStructFieldCls(fieldType: Int): Boolean = {
        fieldType match {
            case CLS_INT          => true
            case CLS_LONG         => true
            case CLS_DOUBLE       => true
            case CLS_STRING       => true
            case CLS_SYSTEMSTRING => true
            case CLS_VSTRING      => true
            case _                => false
        }
    }
}

class StructField(val name: String, val cls: Int, val len: Int, val fieldInfo: FieldInfo)
class StructDef {
    val fields = ArrayBuffer[StructField]()
    val keys = HashSet[String]()
}
class ObjectDef {
    val fields = ArrayBuffer[TlvType]()
    val keyToTypeMap = HashMapStringString()
    val typeToKeyMap = HashMapStringString()
    val keyToFieldMap = HashMap[String, FieldInfo]()
}

class TlvType(val name: String, val cls: Int, val code: Int, val fieldInfo: FieldInfo,
              val itemType: TlvType = TlvType.UNKNOWN, // for array
              val structDef: StructDef = TlvType.EMPTY_STRUCTDEF, // for struct
              val objectDef: ObjectDef = TlvType.EMPTY_OBJECTDEF // for object 
              ) {

    import TlvType._

    def defaultValue = if (fieldInfo == null) null else fieldInfo.defaultValue

    def hasSubFieldInfo(): Boolean = {

        cls match {
            case CLS_STRINGARRAY | CLS_INTARRAY | CLS_LONGARRAY | CLS_DOUBLEARRAY =>
                itemType.fieldInfo != null
            case CLS_STRUCT =>
                structDef.fields.exists(_.fieldInfo != null)
            case CLS_STRUCTARRAY =>
                itemType.fieldInfo != null || itemType.hasSubFieldInfo()
            case _ =>
                false
        }
    }

    override def toString() = {
        name + "," + cls + "," + code
    }
}

object TlvCodec {

    val EMPTY_STRINGMAP = new HashMapStringString()
    val EMPTY_BUFFER = ChannelBuffers.buffer(0)

    def hexDump(buff: ChannelBuffer): String = {
        val s = ChannelBuffers.hexDump(buff, 0, buff.writerIndex)
        val sb = new StringBuilder()
        var i = 0
        var cnt = 0
        sb.append("\n")
        while( i < s.length ) {
            sb.append(s.charAt(i))
            i += 1
            cnt += 1
            if( (cnt%2) == 0 ) sb.append(" ")
            if( (cnt%8) == 0 ) sb.append("  ")
            if( (cnt%16) == 0 ) sb.append("\n")
        }
        sb.toString
    }

    def writePad(buff: ChannelBuffer) {
        val pad = aligned(buff.writerIndex) - buff.writerIndex
        pad match {
            case 0 =>
            case 1 =>
                buff.writeByte(0)
            case 2 =>
                buff.writeByte(0)
                buff.writeByte(0)
            case 3 =>
                buff.writeByte(0)
                buff.writeByte(0)
                buff.writeByte(0)
            case _ =>
        }
    }

    def skipRead(buff: ChannelBuffer, n: Int) {
        val newposition = Math.min(buff.readerIndex + n, buff.writerIndex)
        buff.readerIndex(newposition)
    }

    def readString(buff: ChannelBuffer, len: Int, encoding: String): String = {
        val bytes = new Array[Byte](len)
        buff.getBytes(buff.readerIndex, bytes)
        skipRead(buff, aligned(len))
        new String(bytes, encoding)
    }

    def getString(buff: ChannelBuffer, len: Int, encoding: String): String = {
        val bytes = new Array[Byte](len)
        buff.getBytes(buff.readerIndex, bytes)
        new String(bytes, encoding)
    }

    def aligned(len: Int): Int = {
        if ((len & 0x03) != 0)
            ((len >> 2) + 1) << 2
        else
            len
    }
}

class TlvCodec(val configFile: String) extends Logging {

    import TlvCodec._
    import TlvType._

    var serviceId: Int = _
    var serviceOrigName: String = _
    var serviceName: String = _
    var version = 1

    val msgIds = ArrayBufferInt()

    val typeCodeToNameMap = new HashMap[Int, TlvType]()
    val typeNameToCodeMap = new HashMap[String, TlvType]()

    val msgIdToNameMap = new HashMap[Int, String]()
    val msgNameToIdMap = new HashMap[String, Int]()
    val msgIdToOrigNameMap = new HashMap[Int, String]()

    val msgKeyToTypeMapForReq = new HashMap[Int, HashMapStringString]()
    val msgTypeToKeyMapForReq = new HashMap[Int, HashMapStringString]()
    val msgKeysForReq = new HashMap[Int, ArrayBufferString]()
    val msgKeyToFieldMapForReq = new HashMap[Int, HashMap[String, FieldInfo]]()

    val msgKeyToTypeMapForRes = new HashMap[Int, HashMapStringString]()
    val msgTypeToKeyMapForRes = new HashMap[Int, HashMapStringString]()
    val msgKeysForRes = new HashMap[Int, ArrayBufferString]()
    val msgKeyToFieldMapForRes = new HashMap[Int, HashMap[String, FieldInfo]]()

    val codecAttributes = new HashMapStringString()
    val msgAttributes = new HashMap[Int, HashMapStringString]()

    init

    def init(): Unit = {
        try {
            initInternal()
        } catch {
            case ex: Exception =>
                log.error("tlv init failed, file={}, error={}", configFile, ex.getMessage);
                throw ex
        }
    }

    def getAttribute(t: Node, field: String): String = {
        (t \ ("@" + field)).toString
    }

    def getValidatorCls(t: Node): String = {
        val validatorCls = getAttribute(t, "validator").toLowerCase
        if (validatorCls != "") return validatorCls
        val required = getAttribute(t, "required")
        if (TypeSafe.isTrue(required)) return "required"
        else return null
    }

    def getFieldInfo(t: Node): FieldInfo = {

        // 严格区分 null 和 空串
        val defaultValue = t.attribute("default") match {
            case Some(a) => a.toString
            case None    => null
        }

        val validatorCls = getValidatorCls(t)
        val validatorParam = getAttribute(t, "validatorParam")
        val returnCode = getAttribute(t, "returnCode")
        val encoderCls0 = getAttribute(t, "encoder")
        val encoderCls = if (encoderCls0 == "") null else encoderCls0
        val encoderParam = getAttribute(t, "encoderParam")
        FieldInfo.getFieldInfo(defaultValue, validatorCls, validatorParam, returnCode, encoderCls, encoderParam)
    }

    def getValidatorInfo(fieldInfo: FieldInfo, tlvType: TlvType): Tuple3[String, String, String] = {
        if (fieldInfo.validator != null) {
            val validatorCls = fieldInfo.validatorCls
            val validatorParam = fieldInfo.validatorParam
            val returnCode = fieldInfo.validatorReturnCode
            (validatorCls, validatorParam, returnCode)
        } else if (tlvType.fieldInfo.validator != null) {
            val validatorCls = tlvType.fieldInfo.validatorCls
            val validatorParam = tlvType.fieldInfo.validatorParam
            val returnCode = tlvType.fieldInfo.validatorReturnCode
            (validatorCls, validatorParam, returnCode)
        } else {
            (null, "", "")
        }
    }

    def getEncoderInfo(fieldInfo: FieldInfo, tlvType: TlvType): Tuple2[String, String] = {
        if (fieldInfo.encoder != null) {
            val encoderCls = fieldInfo.encoderCls
            val encoderParam = fieldInfo.encoderParam
            (encoderCls, encoderParam)
        } else if (tlvType.fieldInfo.encoder != null) {
            val encoderCls = tlvType.fieldInfo.encoderCls
            val encoderParam = tlvType.fieldInfo.encoderParam
            (encoderCls, encoderParam)
        } else {
            (null, "")
        }
    }

    def getFieldInfo(t: Node, tlvType: TlvType): FieldInfo = {

        val fieldInfo = getFieldInfo(t)
        if (fieldInfo == null) return tlvType.fieldInfo
        if (tlvType.fieldInfo == null) return fieldInfo

        val defaultValue = if (fieldInfo.defaultValue != null) fieldInfo.defaultValue else tlvType.fieldInfo.defaultValue
        val (validatorCls, validatorParam, returnCode) = getValidatorInfo(fieldInfo, tlvType)
        val (encoderCls, encoderParam) = getEncoderInfo(fieldInfo, tlvType)

        FieldInfo.getFieldInfo(defaultValue, validatorCls, validatorParam, returnCode, encoderCls, encoderParam)
    }

    def getStructType(t: Node): String = {
        val s = (t \ "@type").toString
        if (s != "") return s
        return "unknown" // 不设默认值
    }

    def getStructLen(t: Node, fieldType: Int): Int = {

        fieldType match {
            case CLS_INT                    => return 4
            case CLS_LONG                   => return 8
            case CLS_DOUBLE                 => return 8
            case CLS_SYSTEMSTRING           => return 0
            case CLS_VSTRING                => return 0
            case CLS_STRING =>
                val s = (t \ "@len").toString
                if (s == "") return -1
                else return s.toInt
            case _ =>
                throw new CodecException("unknown type for struct, type=%d".format(fieldType))
        }

    }

    def checkStructLen(name: String, structFields: ArrayBuffer[StructField]) {
        for (i <- 0 until structFields.length - 1) { // don't include the last field
            if (structFields(i).len == -1) {
                throw new CodecException("struct length not valid,name=%s,serviceId=%d".format(name, serviceId))
            }
        }
    }

    def initInternal(): Unit = {

        val cfgXml = XML.load(configFile)

        serviceId = (cfgXml \ "@id").toString.toInt
        serviceOrigName = (cfgXml \ "@name").toString
        serviceName = serviceOrigName.toLowerCase

        val versionStr = (cfgXml \ "@version").toString
        if (versionStr != "") version = versionStr.toInt
        if (version != 1 && version != 2) {
            throw new CodecException("version not valid, serviceOrigName=%s".format(serviceOrigName))
        }

        val metadatas = cfgXml.attributes.filter(_.key != "id").filter(_.key != "name").filter(_.key != "version")
        for (m <- metadatas) {
            codecAttributes.put(m.key, m.value.text)
        }

        // 本地缓存插件使用此节点
        val codecSql = (cfgXml \ "sql").text.trim
        if (codecSql != "")
            codecAttributes.put("sql", codecSql)

        // if( codecAttributes.size > 0 ) println( codecAttributes )

        val types = cfgXml \ "type"

        for (t <- types) {

            val name = (t \ "@name").toString.toLowerCase
            val cls = clsToInt((t \ "@class").toString, (t \ "@isbytes").toString)
            if (cls == CLS_UNKNOWN || cls == CLS_SYSTEMSTRING || cls == CLS_VSTRING)
                throw new CodecException("class not valid, class=%s".format((t \ "@class").toString))
            val code = if (cls != CLS_ARRAY) (t \ "@code").toString.toInt else 0

            val metadatas = t.attributes.filter(_.key != "code").filter(_.key != "name").filter(_.key != "class").filter(_.key != "isbytes")
            for (m <- metadatas) {
                codecAttributes.put("type-" + name + "-" + m.key, m.value.text)
            }

            val fieldInfo = getFieldInfo(t)

            cls match {

                case CLS_ARRAY =>

                    val itemTypeName = (t \ "@itemType").toString.toLowerCase
                    val itemTlvType = typeNameToCodeMap.getOrElse(itemTypeName, UNKNOWN)
                    if (itemTlvType == UNKNOWN) {
                        throw new CodecException("itemType not valid,name=%s,itemType=%s".format(name, itemTypeName))
                    }
                    val arraycls = clsToArrayType(itemTlvType.cls)
                    if (arraycls == CLS_UNKNOWN) {
                        throw new CodecException("itemType not valid,name=%s,itemType=%s".format(name, itemTypeName))
                    }
                    val tlvType = new TlvType(name, arraycls, itemTlvType.code, fieldInfo, itemTlvType, itemTlvType.structDef, itemTlvType.objectDef)

                    if (typeCodeToNameMap.getOrElse(tlvType.code, null) != null) {
                        val foundTlvType = typeCodeToNameMap.getOrElse(tlvType.code, null)
                        if (isArray(foundTlvType.cls))
                            throw new CodecException("multiple array definitions for one code!!! code=%d,serviceId=%d".format(tlvType.code, serviceId))
                    }
                    typeCodeToNameMap.put(tlvType.code, tlvType); // overwrite itemtype's map

                    if (typeNameToCodeMap.getOrElse(tlvType.name, null) != null) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(tlvType.name, serviceId))
                    }
                    typeNameToCodeMap.put(tlvType.name, tlvType);

                    val classex = codecAttributes.getOrElse("classex-" + itemTypeName, null)
                    if (classex != null) {
                        codecAttributes.put("classex-" + name, classex)
                    }

                case CLS_STRUCT =>

                    val structFields = ArrayBuffer[StructField]()

                    val fields = t \ "field"

                    for (f <- fields) {

                        val fieldName = (f \ "@name").toString
                        val fieldTypeName = getStructType(f)
                        val fieldType = clsToInt(fieldTypeName)
                        if (!checkStructFieldCls(fieldType)) {
                            throw new CodecException("not supported field type,name=%s,type=%s,serviceId=%d".format(name, clsToName(fieldType),serviceId))
                        }

                        val fieldLen = getStructLen(f, fieldType)
                        val fieldInfo2 = getFieldInfo(f)

                        val sf = new StructField(fieldName, fieldType, fieldLen, fieldInfo2)
                        structFields += sf

                        val classex = (f \ "@classex").toString
                        if (classex != "") {
                            codecAttributes.put("classex-" + name + "-" + fieldName, classex)
                            codecAttributes.put("classex-" + name, "some") // 做标记,简化处理时扫描
                        }

                        val fieldMetaDatas = f.attributes.filter(_.key != "name").filter(_.key != "type").filter(_.key != "len")
                        for (m <- fieldMetaDatas) {
                            codecAttributes.put("type-" + name + "-" + fieldName + "-" + m.key, m.value.text)
                        }
                    }

                    checkStructLen(name, structFields)

                    val structDef = new StructDef()
                    structDef.fields ++= structFields
                    structDef.fields.foreach(f => structDef.keys.add(f.name))
                    val tlvType = new TlvType(name, cls, code, fieldInfo, structDef = structDef)

                    if (typeCodeToNameMap.getOrElse(tlvType.code, null) != null) {
                        throw new CodecException("code duplicated, code=%d,serviceId=%d".format(tlvType.code, serviceId))
                    }

                    if (typeNameToCodeMap.getOrElse(tlvType.name, null) != null) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(tlvType.name, serviceId))
                    }

                    typeCodeToNameMap.put(tlvType.code, tlvType);
                    typeNameToCodeMap.put(tlvType.name, tlvType);

                case CLS_OBJECT =>

                    val t_keyToTypeMap = new HashMapStringString()
                    val t_typeToKeyMap = new HashMapStringString()
                    val t_tlvTypes = new ArrayBuffer[TlvType]()
                    val t_keyToFieldMap = new HashMap[String, FieldInfo]()

                    val fields = t \ "field"

                    for (f <- fields) {

                        val key = (f \ "@name").toString
                        val typeName0 = (f \ "@type").toString.toLowerCase
                        val typeName = if (typeName0 == "") (key + "_type").toLowerCase else typeName0

                        val tlvType = typeNameToCodeMap.getOrElse(typeName, null)
                        if (tlvType == null) {
                            throw new CodecException("typeName %s not found".format(typeName));
                        }

                        if (t_keyToTypeMap.getOrElse(key, null) != null) {
                            throw new CodecException("key duplicated, key=%s,serviceId=%d".format(key, serviceId))
                        }
                        if (t_typeToKeyMap.getOrElse(typeName, null) != null) {
                            throw new CodecException("type duplicated, type=%s,serviceId=%d".format(typeName, serviceId))
                        }

                        t_keyToTypeMap.put(key, typeName)
                        t_typeToKeyMap.put(typeName, key)
                        t_tlvTypes += tlvType
                        val fieldInfo2 = getFieldInfo(f, tlvType)
                        if (fieldInfo2 != null) {
                            t_keyToFieldMap.put(key, fieldInfo2)
                        }

                        val classex = (f \ "@classex").toString
                        if (classex != "") {
                            codecAttributes.put("classex-" + name + "-" + key, classex)
                            codecAttributes.put("classex-" + name, "some") // 做标记,简化处理时扫描
                        }

                        val fieldMetaDatas = f.attributes.filter(_.key != "name").filter(_.key != "type")
                        for (m <- fieldMetaDatas) {
                            codecAttributes.put("type-" + name + "-" + key + "-" + m.key, m.value.text)
                        }
                    }

                    val objectDef = new ObjectDef()
                    objectDef.fields ++= t_tlvTypes
                    objectDef.keyToTypeMap ++= t_keyToTypeMap
                    objectDef.typeToKeyMap ++= t_typeToKeyMap
                    objectDef.keyToFieldMap ++= t_keyToFieldMap

                    val tlvType = new TlvType(name, cls, code, fieldInfo, objectDef = objectDef)

                    if (typeCodeToNameMap.getOrElse(tlvType.code, null) != null) {
                        throw new CodecException("code duplicated, code=%d,serviceId=%d".format(tlvType.code, serviceId))
                    }

                    if (typeNameToCodeMap.getOrElse(tlvType.name, null) != null) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(tlvType.name, serviceId))
                    }

                    typeCodeToNameMap.put(tlvType.code, tlvType);
                    typeNameToCodeMap.put(tlvType.name, tlvType);

                case _ =>

                    if (!checkTypeCls(cls)) {
                        throw new CodecException("not supported type,name=%s,type=%s".format(name, clsToName(cls)))
                    }

                    val tlvType = new TlvType(name, cls, code, fieldInfo)

                    if (typeCodeToNameMap.getOrElse(tlvType.code, null) != null) {
                        throw new CodecException("code duplicated, code=%d,serviceId=%d".format(tlvType.code, serviceId))
                    }

                    if (typeNameToCodeMap.getOrElse(tlvType.name, null) != null) {
                        throw new CodecException("name duplicated, name=%s,serviceId=%d".format(tlvType.name, serviceId))
                    }

                    typeCodeToNameMap.put(tlvType.code, tlvType);
                    typeNameToCodeMap.put(tlvType.name, tlvType);

                    val classex = (t \ "@classex").toString
                    if (classex != "") {
                        codecAttributes.put("classex-" + name, classex)
                    }

            }

            val arrayTypeName = (t \ "@array").toString.toLowerCase // 可以在type上直接加array属性申明数组类型
            if (arrayTypeName != "" && cls != CLS_ARRAY) {

                val itemTlvType = typeNameToCodeMap.getOrElse(name, UNKNOWN)
                val arraycls = clsToArrayType(itemTlvType.cls)
                if (arraycls == CLS_UNKNOWN) {
                    throw new CodecException("not allowed class for array")
                }
                val tlvType = new TlvType(arrayTypeName, arraycls, itemTlvType.code, null, itemTlvType, itemTlvType.structDef, itemTlvType.objectDef)

                if (typeCodeToNameMap.getOrElse(tlvType.code, null) != null) {
                    val foundTlvType = typeCodeToNameMap.getOrElse(tlvType.code, null)
                    if (isArray(foundTlvType.cls))
                        throw new CodecException("multiple array definitions for one code!!! code=%d,serviceId=%d".format(tlvType.code, serviceId))
                }
                typeCodeToNameMap.put(tlvType.code, tlvType); // overwrite itemtype's map

                if (typeNameToCodeMap.getOrElse(tlvType.name, null) != null) {
                    throw new CodecException("name duplicated, name=%s,serviceId=%d".format(tlvType.name, serviceId))
                }
                typeNameToCodeMap.put(tlvType.name, tlvType);
            }
        }

        val messages = cfgXml \ "message"

        for (t <- messages) {

            val attributes = new HashMapStringString()

            val msgId = (t \ "@id").toString.toInt
            val msgNameOrig = (t \ "@name").toString
            val msgName = msgNameOrig.toLowerCase

            msgIds += msgId

            if (msgIdToNameMap.getOrElse(msgId, null) != null) {
                throw new CodecException("msgId duplicated, msgId=%d,serviceId=%d".format(msgId, serviceId))
            }

            if (msgNameToIdMap.getOrElse(msgName, null) != null) {
                throw new CodecException("msgName duplicated, msgName=%s,serviceId=%d".format(msgName, serviceId))
            }

            msgIdToNameMap.put(msgId, msgName)
            msgNameToIdMap.put(msgName, msgId)
            msgIdToOrigNameMap.put(msgId, msgNameOrig)

            val metadatas = t.attributes.filter(_.key != "id").filter(_.key != "name")
            for (m <- metadatas) {
                attributes.put(m.key, m.value.text)
            }

            // 用于db插件
            val sql = (t \ "sql").text.trim
            if (sql != "")
                attributes.put("sql", sql)

            val fieldsReq = t \ "requestParameter" \ "field"
            val t_keyToTypeMapForReq = new HashMapStringString()
            val t_typeToKeyMapForReq = new HashMapStringString()
            val t_keysForReq = new ArrayBufferString()
            val t_keyToFieldMapForReq = new HashMap[String, FieldInfo]()

            for (f <- fieldsReq) {
                val key = (f \ "@name").toString
                val typeName0 = (f \ "@type").toString.toLowerCase
                val typeName = if (typeName0 == "") (key + "_type").toLowerCase else typeName0

                val tlvType = typeNameToCodeMap.getOrElse(typeName, null)
                if (tlvType == null) {
                    throw new CodecException("typeName %s not found".format(typeName));
                }

                if (t_keyToTypeMapForReq.getOrElse(key, null) != null) {
                    throw new CodecException("req key duplicated, key=%s,serviceId=%d,msgId=%d".format(key, serviceId, msgId))
                }
                if (t_typeToKeyMapForReq.getOrElse(typeName, null) != null) {
                    throw new CodecException("req type duplicated, type=%s,serviceId=%d,msgId=%d".format(typeName, serviceId, msgId))
                }

                t_keyToTypeMapForReq.put(key, typeName)
                t_typeToKeyMapForReq.put(typeName, key)
                t_keysForReq += key
                val fieldInfo = getFieldInfo(f, tlvType)
                if (fieldInfo != null) {
                    t_keyToFieldMapForReq.put(key, fieldInfo)
                } else if (tlvType.hasSubFieldInfo()) {
                    t_keyToFieldMapForReq.put(key, null)
                }

                val metadatas = f.attributes.filter(_.key != "name").filter(_.key != "type")
                for (m <- metadatas) {
                    attributes.put("req-" + key + "-" + m.key, m.value.text)
                }

            }
            msgKeyToTypeMapForReq.put(msgId, t_keyToTypeMapForReq)
            msgTypeToKeyMapForReq.put(msgId, t_typeToKeyMapForReq)
            msgKeysForReq.put(msgId, t_keysForReq)
            if (t_keyToFieldMapForReq.size > 0)
                msgKeyToFieldMapForReq.put(msgId, t_keyToFieldMapForReq)

            val fieldsRes = t \ "responseParameter" \ "field"
            val t_keyToTypeMapForRes = new HashMapStringString()
            val t_typeToKeyMapForRes = new HashMapStringString()
            val t_keysForRes = new ArrayBufferString()
            val t_keyToFieldMapForRes = new HashMap[String, FieldInfo]()

            for (f <- fieldsRes) {
                val key = (f \ "@name").toString
                val typeName0 = (f \ "@type").toString.toLowerCase
                val typeName = if (typeName0 == "") (key + "_type").toLowerCase else typeName0

                val tlvType = typeNameToCodeMap.getOrElse(typeName, null)
                if (tlvType == null) {
                    throw new CodecException("typeName %s not found".format(typeName));
                }

                if (t_keyToTypeMapForRes.getOrElse(key, null) != null) {
                    throw new CodecException("res key duplicated, key=%s,serviceId=%d,msgId=%d".format(key, serviceId, msgId))
                }
                if (t_typeToKeyMapForRes.getOrElse(typeName, null) != null) {
                    throw new CodecException("res type duplicated, type=%s,serviceId=%d,msgId=%d".format(typeName, serviceId, msgId))
                }

                t_keyToTypeMapForRes.put(key, typeName)
                t_typeToKeyMapForRes.put(typeName, key)
                t_keysForRes += key
                val fieldInfo = getFieldInfo(f, tlvType)
                if (fieldInfo != null) {
                    t_keyToFieldMapForRes.put(key, fieldInfo)
                } else if (tlvType.hasSubFieldInfo()) {
                    t_keyToFieldMapForRes.put(key, null)
                }

                val metadatas = f.attributes.filter(_.key != "name").filter(_.key != "type")
                for (m <- metadatas) {
                    attributes.put("res-" + key + "-" + m.key, m.value.text)
                }
            }

            msgKeyToTypeMapForRes.put(msgId, t_keyToTypeMapForRes)
            msgTypeToKeyMapForRes.put(msgId, t_typeToKeyMapForRes)
            msgKeysForRes.put(msgId, t_keysForRes)
            if (t_keyToFieldMapForRes.size > 0)
                msgKeyToFieldMapForRes.put(msgId, t_keyToFieldMapForRes)

            msgAttributes.put(msgId, attributes)
        }

    }

    def allTlvTypes(): List[TlvType] = {
        typeNameToCodeMap.values.toList
    }
    def allMsgIds(): List[Int] = {
        msgKeyToTypeMapForReq.keys.toList
    }
    def findTlvType(code: Int): TlvType = {
        typeCodeToNameMap.getOrElse(code, null)
    }
    def msgIdToName(msgId: Int): String = {
        msgIdToNameMap.getOrElse(msgId, null)
    }
    def msgNameToId(msgName: String): Int = {
        msgNameToIdMap.getOrElse(msgName, 0)
    }

    def decodeRequest(msgId: Int, buff: ChannelBuffer, encoding: Int): Tuple2[HashMapStringAny, Int] = {
        try {
            val typeMap = msgTypeToKeyMapForReq.getOrElse(msgId, EMPTY_STRINGMAP)
            val keyMap = msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP)
            val fieldMap = msgKeyToFieldMapForReq.getOrElse(msgId, null)
            return decode(typeMap, keyMap, fieldMap,
                buff, 0, buff.writerIndex, encoding, needEncode = true)
        } catch {
            case e: Throwable =>
                log.error("decode request error", e)
                return (HashMapStringAny(), ResultCodes.TLV_DECODE_ERROR)
        }
    }

    def decodeResponse(msgId: Int, buff: ChannelBuffer, encoding: Int): Tuple2[HashMapStringAny, Int] = {
        try {
            val typeMap = msgTypeToKeyMapForRes.getOrElse(msgId, EMPTY_STRINGMAP)
            val keyMap = msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP)
            val fieldMap = msgKeyToFieldMapForRes.getOrElse(msgId, null)
            return decode(typeMap, keyMap, fieldMap,
                buff, 0, buff.writerIndex, encoding, needEncode = false)
        } catch {
            case e: Throwable =>
                log.error("decode response error", e)
                return (HashMapStringAny(), ResultCodes.TLV_DECODE_ERROR)
        }
    }

    def decode(typeMap: HashMapStringString, keyMap: HashMapStringString, fieldMap: HashMap[String, FieldInfo],
               buff: ChannelBuffer, start: Int, limit: Int, encoding: Int, needEncode: Boolean): Tuple2[HashMapStringAny, Int] = {

        buff.readerIndex(start)

        val map = new HashMapStringAny()
        var brk = false
        var errorCode = 0

        while (buff.readerIndex + 4 <= limit && !brk) {

            val code = buff.readShort.toInt;
            var len: Int = buff.readShort & 0xffff;
            var tlvheadlen = 4
            if (code > 0 && len == 0) { 
                len = buff.readInt; 
                tlvheadlen = 8; 
            }

            if (len < tlvheadlen) { // 兼容特殊情况
                if (code == 0 && len == 0) { // 00 03 00 04 00 00 00 00, the last 4 bytes are padding bytes by session server

                } else {
                    log.error("length_error,code=" + code + ",len=" + len + ",limit=" + limit + ",map=" + map.toString);
                    if (log.isDebugEnabled) {
                        log.debug("body bytes=" + hexDump(buff))
                    }
                }
                brk = true
            }
            if (buff.readerIndex + len - tlvheadlen > limit) {
                log.error("length_error,code=" + code + ",len=" + len + ",limit=" + limit + ",map=" + map.toString);
                if (log.isDebugEnabled) {
                    log.debug("body bytes=" + hexDump(buff))
                }
                brk = true
            }

            if (!brk) {

                var tlvType = typeCodeToNameMap.getOrElse(code, UNKNOWN)

                var key = typeMap.getOrElse(tlvType.name, null)

                // if is array, check to see if is itemType

                if (key == null && isArray(tlvType.cls)) {
                    key = typeMap.getOrElse(tlvType.itemType.name, null)
                    if (key != null) {
                        tlvType = tlvType.itemType
                    }
                }

                if ( (tlvType eq UNKNOWN) || key == null) {

                    skipRead(buff, aligned(len) - tlvheadlen)

                } else {

                    tlvType.cls match {

                        case CLS_INT => {
                            if (len != 8) throw new CodecException("int_length_error,len=" + len)
                            val value = buff.readInt
                            map.put(key, value)
                        }

                        case CLS_LONG => {
                            if (len != 12) throw new CodecException("long_length_error,len=" + len)
                            val value = buff.readLong
                            map.put(key, value)
                        }

                        case CLS_DOUBLE => {
                            if (len != 12) throw new CodecException("double_length_error,len=" + len)
                            val value = buff.readDouble
                            map.put(key, value)
                        }

                        case CLS_STRING => {
                            val value = readString(buff, len - tlvheadlen, AvenueCodec.toEncoding(encoding))
                            map.put(key, value)
                        }

                        case CLS_BYTES => {
                            val p = buff.readerIndex
                            val value = new Array[Byte](len - tlvheadlen)
                            buff.readBytes(value)
                            map.put(key, value)

                            val newposition = Math.min(p + aligned(len) - tlvheadlen, buff.writerIndex)
                            buff.readerIndex(newposition)
                        }

                        case CLS_STRUCT => {
                            val value = decodeStruct(buff, len - tlvheadlen, tlvType, encoding)
                            map.put(key, value)
                        }

                        case CLS_OBJECT => {
                            val p = buff.readerIndex
                            val (value, ec) = decode(tlvType.objectDef.typeToKeyMap, tlvType.objectDef.keyToTypeMap, tlvType.objectDef.keyToFieldMap,
                                buff, p, p + len - tlvheadlen, encoding, needEncode)
                            map.put(key, value)
                            if (ec != 0 && errorCode == 0) errorCode = ec

                            val newposition = Math.min(p + aligned(len) - tlvheadlen, buff.writerIndex)
                            buff.readerIndex(newposition)
                        }

                        case CLS_INTARRAY => {
                            if (len != 8) throw new CodecException("int_length_error,len=" + len)
                            val value = buff.readInt

                            var a = map.getOrElse(key, null)
                            if (a == null) {
                                val aa = new ArrayBufferInt()
                                aa += value
                                map.put(key, aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferInt]
                                aa += value
                            }
                        }

                        case CLS_LONGARRAY => {
                            if (len != 12) throw new CodecException("long_length_error,len=" + len)
                            val value = buff.readLong

                            var a = map.getOrElse(key, null)
                            if (a == null) {
                                val aa = new ArrayBufferLong()
                                aa += value
                                map.put(key, aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferLong]
                                aa += value
                            }
                        }

                        case CLS_DOUBLEARRAY => {
                            if (len != 12) throw new CodecException("double_length_error,len=" + len)
                            val value = buff.readDouble

                            var a = map.getOrElse(key, null)
                            if (a == null) {
                                val aa = new ArrayBufferDouble()
                                aa += value
                                map.put(key, aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferDouble]
                                aa += value
                            }
                        }

                        case CLS_STRINGARRAY => {
                            val value = readString(buff, len - tlvheadlen, AvenueCodec.toEncoding(encoding))

                            var a = map.getOrElse(key, null)
                            if (a == null) {
                                val aa = new ArrayBufferString()
                                aa += value
                                map.put(key, aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferString]
                                aa += value
                            }
                        }

                        case CLS_STRUCTARRAY => {

                            val value = decodeStruct(buff, len - tlvheadlen, tlvType, encoding)

                            var a = map.getOrElse(key, null)
                            if (a == null) {
                                val aa = new ArrayBufferMap()
                                aa += value
                                map.put(key, aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferMap]
                                aa += value
                            }
                        }

                        case CLS_OBJECTARRAY => {

                            val p = buff.readerIndex
                            val (value, ec) = decode(tlvType.objectDef.typeToKeyMap, tlvType.objectDef.keyToTypeMap, tlvType.objectDef.keyToFieldMap,
                                buff, p, len - tlvheadlen, encoding, needEncode)

                            var a = map.getOrElse(key, null)
                            if (a == null) {
                                val aa = new ArrayBufferMap()
                                aa += value
                                map.put(key, aa)
                            } else {
                                val aa = a.asInstanceOf[ArrayBufferMap]
                                aa += value
                            }

                            if (ec != 0 && errorCode == 0) errorCode = ec

                            val newposition = Math.min(p + aligned(len) - tlvheadlen, buff.writerIndex)
                            buff.readerIndex(newposition)
                        }
                        case _ => {
                            skipRead(buff, aligned(len) - tlvheadlen)
                        }
                    }
                }
            }
        }

        val ec = validate(keyMap, fieldMap, map, needEncode)
        if (ec != 0 && errorCode == 0) errorCode = ec
        (map, errorCode)
    }

    def decodeStruct(buff: ChannelBuffer, maxLen: Int, tlvType: TlvType, encoding: Int): HashMapStringAny = {

        val map = new HashMapStringAny()

        var totalLen = 0
        for (i <- 0 until tlvType.structDef.fields.length) {
            val f = tlvType.structDef.fields(i)
            val key = f.name
            val t = f.cls
            var len = f.len

            if (len == -1) len = maxLen - totalLen; // last field

            totalLen += len
            if (totalLen > maxLen) {
                throw new CodecException("struct_data_not_valid")
            }

            t match {

                case CLS_INT => {
                    val value = buff.readInt
                    map.put(key, value)
                }
                case CLS_LONG => {
                    val value = buff.readLong
                    map.put(key, value)
                }
                case CLS_DOUBLE => {
                    val value = buff.readDouble
                    map.put(key, value)
                }
                case CLS_STRING => {
                    val value = readString(buff, len, AvenueCodec.toEncoding(encoding)).trim()
                    map.put(key, value)
                }
                case CLS_SYSTEMSTRING => {
                    len = buff.readInt // length for system string
                    totalLen += 4
                    totalLen += aligned(len)
                    if (totalLen > maxLen) {
                        throw new CodecException("struct_data_not_valid")
                    }
                    val value = readString(buff, len, AvenueCodec.toEncoding(encoding)).trim()
                    map.put(key, value)
                }
                case CLS_VSTRING => {

                    var t = buff.readShort & 0xffff
                    if (t == 0xffff) {
                        len = buff.readInt
                        totalLen += aligned(6 + len)
                        if (totalLen > maxLen) {
                            throw new CodecException("struct_data_not_valid")
                        }
                        val value = getString(buff, len, AvenueCodec.toEncoding(encoding)).trim()
                        map.put(key, value)

                        skipRead(buff, aligned(6 + len) - 6)
                    } else {
                        len = t
                        totalLen += aligned(2 + len)
                        if (totalLen > maxLen) {
                            throw new CodecException("struct_data_not_valid")
                        }
                        val value = getString(buff, len, AvenueCodec.toEncoding(encoding)).trim()
                        map.put(key, value)

                        skipRead(buff, aligned(2 + len) - 2)
                    }

                }

                case _ =>

                    log.error("unknown type")
            }
        }

        map
    }

    def encodeRequest(msgId: Int, map: HashMapStringAny, encoding: Int): Tuple2[ChannelBuffer, Int] = {
        try {
            val keyMap = msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP)
            val fieldMap = msgKeyToFieldMapForReq.getOrElse(msgId, null)
            val buff = ChannelBuffers.dynamicBuffer(256)
            val ec = encode(buff, keyMap, fieldMap, map, encoding, needEncode = false)
            return (buff, ec)
        } catch {
            case e: Throwable =>
                log.error("encode request error", e)
                return (EMPTY_BUFFER, ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def encodeResponse(msgId: Int, map: HashMapStringAny, encoding: Int): Tuple2[ChannelBuffer, Int] = {
        try {
            val keyMap = msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP)
            val fieldMap = msgKeyToFieldMapForRes.getOrElse(msgId, null)
            val buff = ChannelBuffers.dynamicBuffer(256)
            val ec = encode(buff, keyMap, fieldMap, map, encoding, needEncode = true)
            return (buff, ec)
        } catch {
            case e: Throwable =>
                log.error("encode response error", e)
                return (EMPTY_BUFFER, ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def encode(buff: ChannelBuffer, keyMap: HashMapStringString, fieldMap: HashMap[String, FieldInfo],
               dataMap: HashMapStringAny, encoding: Int,
               needEncode: Boolean): Int = {

        var errorCode = 0
        val ec = validate(keyMap, fieldMap, dataMap, needEncode)
        if (ec != 0 && errorCode == 0) errorCode = ec

        for ((key, value) <- dataMap if value != null) {

            val typeName = keyMap.getOrElse(key, null)
            if (typeName != null) {

                val tlvType = typeNameToCodeMap.getOrElse(typeName, UNKNOWN)

                tlvType.cls match {

                    case CLS_INT =>
                        encodeInt(buff, tlvType.code, value)

                    case CLS_LONG =>
                        encodeLong(buff, tlvType.code, value)

                    case CLS_DOUBLE =>
                        encodeDouble(buff, tlvType.code, value)

                    case CLS_STRING =>
                        encodeString(buff, tlvType.code, value, encoding)

                    case CLS_STRUCT =>
                        encodeStruct(buff, tlvType, value, encoding)

                    case CLS_OBJECT =>
                        val ec = encodeObject(buff, tlvType, value, encoding, needEncode)
                        if (ec != 0 && errorCode == 0) errorCode = ec

                    case CLS_INTARRAY | CLS_LONGARRAY | CLS_DOUBLEARRAY | CLS_STRINGARRAY
                        | CLS_STRUCTARRAY | CLS_OBJECTARRAY =>

                        val ec = encodeArray(buff, tlvType, value, encoding, needEncode)
                        if (ec != 0 && errorCode == 0) errorCode = ec

                    case CLS_BYTES =>
                        if (!value.isInstanceOf[Array[Byte]])
                            throw new CodecException("not supported type for bytes");
                        encodeBytes(buff, tlvType.code, value)

                    case _ =>
                }
            }
        }
        errorCode
    }

    def encodeInt(buff: ChannelBuffer, code: Int, v: Any): Unit = {
        val value = TypeSafe.anyToInt(v)
        buff.writeShort(code.toShort)
        buff.writeShort(8.toShort)
        buff.writeInt(value)
    }

    def encodeLong(buff: ChannelBuffer, code: Int, v: Any): Unit = {
        val value = TypeSafe.anyToLong(v)
        buff.writeShort(code.toShort)
        buff.writeShort(12.toShort)
        buff.writeLong(value)
    }

    def encodeDouble(buff: ChannelBuffer, code: Int, v: Any): Unit = {
        val value = TypeSafe.anyToDouble(v)
        buff.writeShort(code.toShort)
        buff.writeShort(12.toShort)
        buff.writeDouble(value)
    }

    def encodeString(buff: ChannelBuffer, code: Int, v: Any, encoding: Int): Unit = {
        val value = TypeSafe.anyToString(v)
        if (value == null) return
        val bytes = value.getBytes(AvenueCodec.toEncoding(encoding))
        var tlvheadlen = 4
        var alignedLen = aligned(bytes.length + tlvheadlen)
        if (alignedLen > 65535) {
            tlvheadlen = 8
            alignedLen = aligned(bytes.length + tlvheadlen)
        }

        buff.writeShort(code.toShort)

        if (tlvheadlen == 8) {
            buff.writeShort(0)
            buff.writeInt(bytes.length + tlvheadlen)
        } else {
            buff.writeShort((bytes.length + tlvheadlen).toShort)
        }

        buff.writeBytes(bytes)
        writePad(buff)
    }

    def encodeBytes(buff: ChannelBuffer, code: Int, v: Any): Unit = {
        val bytes = v.asInstanceOf[Array[Byte]]
        var tlvheadlen = 4
        var alignedLen = aligned(bytes.length + tlvheadlen)
        if (alignedLen > 65535) {
            tlvheadlen = 8
            alignedLen = aligned(bytes.length + tlvheadlen)
        }

        buff.writeShort(code.toShort)

        if (tlvheadlen == 8) {
            buff.writeShort(0)
            buff.writeInt(bytes.length + tlvheadlen)
        } else {
            buff.writeShort((bytes.length + tlvheadlen).toShort)
        }

        buff.writeBytes(bytes)
        writePad(buff)
    }

    def encodeStruct(buff: ChannelBuffer, tlvType: TlvType, v: Any, encoding: Int) {

        if (!v.isInstanceOf[HashMapStringAny]) {
            throw new CodecException("not supported type in encodeStruct, type=%s".format(v.getClass.getName));
        }

        val datamap = v.asInstanceOf[HashMapStringAny]

        buff.writeShort(tlvType.code.toShort)
        buff.writeShort(0) // 后面再更新为实际值
        val ps = buff.writerIndex
        
        for (i <- 0 until tlvType.structDef.fields.length) {

            val f = tlvType.structDef.fields(i)
            val key = f.name
            val t = f.cls
            val len = f.len

            if (!datamap.contains(key)) {
                val allKeys = tlvType.structDef.keys
                val missedKeys = allKeys.filter(!datamap.contains(_))
                throw new CodecException("struct_not_valid, struct names= " + allKeys.mkString(",") + ", missed keys=" + missedKeys.mkString(","));
            }

            var v = datamap.getOrElse(key, "")

            t match {

                case CLS_INT => 
                    buff.writeInt(TypeSafe.anyToInt(v))
                case CLS_LONG => 
                    buff.writeLong(TypeSafe.anyToLong(v))
                case CLS_DOUBLE => 
                    buff.writeDouble(TypeSafe.anyToDouble(v))
                case CLS_STRING => {

                    if (v == null) v = ""
                    val s = TypeSafe.anyToString(v).getBytes(AvenueCodec.toEncoding(encoding))

                    var actuallen = s.length
                    if (len == -1 || s.length == len) {
                        buff.writeBytes(s)
                    } else if (s.length < len) {
                        buff.writeBytes(s)
                        buff.writeBytes(new Array[Byte](len - s.length))
                    } else {
                        throw new CodecException("string_too_long");
                    }
                    var alignedLen = aligned(len)
                    if (alignedLen != len) {
                        buff.writeBytes(new Array[Byte](alignedLen - len))
                    }
                }
                case CLS_SYSTEMSTRING => {

                    if (v == null) v = ""
                    val s = TypeSafe.anyToString(v).getBytes(AvenueCodec.toEncoding(encoding))

                    buff.writeInt(TypeSafe.anyToInt(s.length))
                    buff.writeBytes(s)
                    
                    var alignedLen = aligned(s.length)
                    if (s.length != alignedLen) {
                        buff.writeBytes(new Array[Byte](alignedLen - s.length))
                    }
                }
                case CLS_VSTRING => {

                    if (v == null) v = ""
                    val s = TypeSafe.anyToString(v).getBytes(AvenueCodec.toEncoding(encoding))

                    if (s.length >= 65535) { // 0xffff
                        var alignedLen = aligned(6 + s.length)
                        buff.writeShort(65535.toShort)
                        buff.writeInt(TypeSafe.anyToInt(s.length))
                        buff.writeBytes(s)
                        if (alignedLen - s.length - 6 > 0) {
                            buff.writeBytes(new Array[Byte](alignedLen - s.length - 6))
                        }
                    } else {
                        var alignedLen = aligned(2 + s.length)
                        buff.writeShort(TypeSafe.anyToInt(s.length).toShort)
                        buff.writeBytes(s)
                        if (alignedLen - s.length - 2 > 0) {
                            buff.writeBytes( new Array[Byte](alignedLen - s.length - 2))
                        }
                    }
                }

                case _ =>
                    log.error("unknown type")
            }

        }
        
        val totalLen = buff.writerIndex - ps
        var tlvheadlen = 4
        var alignedLen = aligned(totalLen + tlvheadlen)

        if (alignedLen > 65535) {
            tlvheadlen = 8
            alignedLen = aligned(totalLen + tlvheadlen)
        }
        
        if (tlvheadlen == 4) {
            buff.setShort(ps - 2, (totalLen + tlvheadlen).toShort) // 写入实际长度
        } else { // struct 需整体向后移动4字节
            buff.writeInt(0)
            val pe = buff.writerIndex
            var i = 0
            while (i < totalLen) {
                buff.setByte(pe - 1 - i, buff.getByte(pe - 1 - i - 4))
                i += 1
            }

            buff.setShort(ps - 2, 0) // 写入0
            buff.setInt(ps, totalLen + tlvheadlen) // 写入实际长度
        }

        writePad(buff)
    }

    def encodeObject(buff: ChannelBuffer, tlvType: TlvType, v: Any, encoding: Int, needEncode: Boolean): Int = {

        if (!v.isInstanceOf[HashMapStringAny]) {
            throw new CodecException("not supported type in encodeObject, type=%s".format(v.getClass.getName));
        }

        val datamap = v.asInstanceOf[HashMapStringAny]

        buff.writeShort(tlvType.code.toShort)
        buff.writeShort(0) // 待定, 后面再写入实际值
        val ps = buff.writerIndex

        val ec = encode(buff, tlvType.objectDef.keyToTypeMap, tlvType.objectDef.keyToFieldMap,
            datamap, encoding, needEncode)

        val totalLen = buff.writerIndex - ps

        var tlvheadlen = 4
        var alignedLen = aligned(totalLen + tlvheadlen)

        if (alignedLen > 65535) {
            tlvheadlen = 8
            alignedLen = aligned(totalLen + tlvheadlen)
        }

        if (tlvheadlen == 4) {
            buff.setShort(ps - 2, (totalLen + tlvheadlen).toShort) // 写入实际长度
        } else { // object 需整体向后移动4字节
            buff.writeInt(0)
            val pe = buff.writerIndex
            var i = 0
            while (i < totalLen) {
                buff.setByte(pe - 1 - i, buff.getByte(pe - 1 - i - 4))
                i += 1
            }

            buff.setShort(ps - 2, 0) // 写入0
            buff.setInt(ps, totalLen + tlvheadlen) // 写入实际长度
        }

        writePad(buff)

        ec
    }

    def encodeArray(buff: ChannelBuffer, tlvType: TlvType, v: Any, encoding: Int, needEncode: Boolean): Int = {

        v match {

            case list: ArrayBuffer[_] =>

                var errorCode = 0
                for (v <- list) {
                    tlvType.cls match {
                        case CLS_INTARRAY =>
                            encodeInt(buff, tlvType.code, v)
                        case CLS_LONGARRAY =>
                            encodeLong(buff, tlvType.code, v)
                        case CLS_DOUBLEARRAY =>
                            encodeDouble(buff, tlvType.code, v)
                        case CLS_STRINGARRAY =>
                            encodeString(buff, tlvType.code, v, encoding)
                        case CLS_STRUCTARRAY =>
                            encodeStruct(buff, tlvType, v, encoding)
                        case CLS_OBJECTARRAY =>
                            val ec = encodeObject(buff, tlvType, v, encoding, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                    }
                }
                return errorCode

            case list: Array[_] =>

                var errorCode = 0
                for (v <- list) {
                    tlvType.cls match {
                        case CLS_INTARRAY =>
                            encodeInt(buff, tlvType.code, v)
                        case CLS_LONGARRAY =>
                            encodeLong(buff, tlvType.code, v)
                        case CLS_DOUBLEARRAY =>
                            encodeDouble(buff, tlvType.code, v)
                        case CLS_STRINGARRAY =>
                            encodeString(buff, tlvType.code, v, encoding)
                        case CLS_STRUCTARRAY =>
                            encodeStruct(buff, tlvType, v, encoding)
                        case CLS_OBJECTARRAY =>
                            val ec = encodeObject(buff, tlvType, v, encoding, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                    }
                }
                return errorCode
            case _ =>
                throw new CodecException("not supported type in encodeArray, type=%s".format(v.getClass.getName));
        }

    }

    def filterRequestInJvm(msgId: Int, dataMap: HashMapStringAny, needEncode: Boolean): Tuple2[HashMapStringAny, Int] = {
        try {
            val keyMap = msgKeyToTypeMapForReq.getOrElse(msgId, EMPTY_STRINGMAP)
            val fieldMap = msgKeyToFieldMapForReq.getOrElse(msgId, null)
            return filter(keyMap, fieldMap, dataMap, needEncode)
        } catch {
            case e: Throwable =>
                log.error("encode request in jvm error", e)
                (HashMapStringAny(), ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def filterResponseInJvm(msgId: Int, dataMap: HashMapStringAny): Tuple2[HashMapStringAny, Int] = {
        try {
            val keyMap = msgKeyToTypeMapForRes.getOrElse(msgId, EMPTY_STRINGMAP)
            val fieldMap = msgKeyToFieldMapForRes.getOrElse(msgId, null)
            return filter(keyMap, fieldMap, dataMap, needEncode = true)
        } catch {
            case e: Throwable =>
                log.error("encode response in jvm error", e)
                (HashMapStringAny(), ResultCodes.TLV_ENCODE_ERROR)
        }
    }

    def filter(keyMap: HashMapStringString, fieldMap: HashMap[String, FieldInfo],
               dataMap: HashMap[String, Any], needEncode: Boolean): Tuple2[HashMapStringAny, Int] = {

        var errorCode = 0

        val ec = validate(keyMap, fieldMap, dataMap, needEncode)
        if (ec != 0 && errorCode == 0) errorCode = ec

        val retmap = new HashMapStringAny();

        for ((key, value) <- dataMap if value != null) {
            val typeName = keyMap.getOrElse(key, null)
            if (typeName != null) {

                val tlvType = typeNameToCodeMap.getOrElse(typeName, UNKNOWN)
                tlvType.cls match {

                    case CLS_INT =>
                        retmap.put(key, TypeSafe.anyToInt(value))
                    case CLS_LONG =>
                        retmap.put(key, TypeSafe.anyToLong(value))
                    case CLS_DOUBLE =>
                        retmap.put(key, TypeSafe.anyToDouble(value))
                    case CLS_STRING =>
                        retmap.put(key, TypeSafe.anyToString(value))
                    case CLS_STRUCT =>
                        retmap.put(key, anyToStruct(tlvType, value))
                    case CLS_OBJECT =>
                        val (m, ec) = anyToObject(tlvType, value, needEncode)
                        retmap.put(key, m)
                        if (ec != 0 && errorCode == 0) errorCode = 0
                    case CLS_INTARRAY =>
                        retmap.put(key, anyToIntArray(value))
                    case CLS_LONGARRAY =>
                        retmap.put(key, anyToLongArray(value))
                    case CLS_DOUBLEARRAY =>
                        retmap.put(key, anyToDoubleArray(value))
                    case CLS_STRINGARRAY =>
                        retmap.put(key, anyToStringArray(value))
                    case CLS_STRUCTARRAY =>
                        retmap.put(key, anyToStructArray(tlvType, value))
                    case CLS_OBJECTARRAY =>
                        val (a, ec) = anyToObjectArray(tlvType, value, needEncode)
                        retmap.put(key, a)
                        if (ec != 0 && errorCode == 0) errorCode = 0
                    case CLS_BYTES =>
                        if (!value.isInstanceOf[Array[Byte]])
                            throw new CodecException("not supported type for bytes");
                        retmap.put(key, value)
                    case _ =>
                }
            }
        }

        (retmap, errorCode)
    }

    def anyToStruct(tlvType: TlvType, v: Any): HashMapStringAny = {

        if (!v.isInstanceOf[HashMapStringAny]) {
            throw new CodecException("not supported type in anyToStruct, type=%s".format(v.getClass.getName));
        }

        val datamap = v.asInstanceOf[HashMapStringAny]
        fastConvertStruct(tlvType, datamap)
        datamap
    }

    def anyToObject(tlvType: TlvType, v: Any, needEncode: Boolean): Tuple2[HashMapStringAny, Int] = {

        if (!v.isInstanceOf[HashMapStringAny]) {
            throw new CodecException("not supported type in anyToObject, type=%s".format(v.getClass.getName));
        }

        val datamap = v.asInstanceOf[HashMapStringAny]

        return filter(tlvType.objectDef.keyToTypeMap, tlvType.objectDef.keyToFieldMap, datamap, needEncode)
    }

    def anyToIntArray(v: Any): ArrayBufferInt = {

        v match {
            case list: ArrayBufferInt =>
                list
            case list: ArrayBuffer[_] =>
                val arr = new ArrayBufferInt()
                list.foreach(arr += TypeSafe.anyToInt(_))
                arr
            case list: Array[_] =>
                val arr = new ArrayBufferInt()
                list.foreach(arr += TypeSafe.anyToInt(_))
                arr
            case _ =>
                throw new CodecException("not supported type in anyToIntArray, type=%s".format(v.getClass.getName));
        }
    }

    def anyToLongArray(v: Any): ArrayBufferLong = {

        v match {
            case list: ArrayBufferLong =>
                list
            case list: ArrayBuffer[_] =>
                val arr = new ArrayBufferLong()
                list.foreach(arr += TypeSafe.anyToLong(_))
                arr
            case list: Array[_] =>
                val arr = new ArrayBufferLong()
                list.foreach(arr += TypeSafe.anyToLong(_))
                arr
            case _ =>
                throw new CodecException("not supported type in anyToLongArray, type=%s".format(v.getClass.getName));
        }
    }

    def anyToDoubleArray(v: Any): ArrayBufferDouble = {

        v match {
            case list: ArrayBufferDouble =>
                list
            case list: ArrayBuffer[_] =>
                val arr = new ArrayBufferDouble()
                list.foreach(arr += TypeSafe.anyToDouble(_))
                arr
            case list: Array[_] =>
                val arr = new ArrayBufferDouble()
                list.foreach(arr += TypeSafe.anyToDouble(_))
                arr
            case _ =>
                throw new CodecException("not supported type in anyToDoubleArray, type=%s".format(v.getClass.getName));
        }
    }

    def anyToStringArray(v: Any): ArrayBufferString = {

        v match {
            case list: ArrayBufferString =>
                list
            case list: ArrayBuffer[_] =>
                val arr = new ArrayBufferString()
                list.foreach(arr += TypeSafe.anyToString(_))
                arr
            case list: Array[_] =>
                val arr = new ArrayBufferString()
                list.foreach(arr += TypeSafe.anyToString(_))
                arr
            case _ =>
                throw new CodecException("not supported type in anyToStringArray, type=%s".format(v.getClass.getName));
        }
    }

    def anyToStructArray(tlvType: TlvType, v: Any): ArrayBufferMap = {

        v match {
            case list: ArrayBufferMap =>
                list.foreach(fastConvertStruct(tlvType, _)) // 相比anyToStruct，不生成对象，直接在内部转换; 查询的时候这种类型特别多
                return list
            case list: ArrayBuffer[_] =>
                val arr = new ArrayBufferMap()
                list.foreach(arr += anyToStruct(tlvType, _))
                arr
            case list: Array[_] =>
                val arr = new ArrayBufferMap()
                list.foreach(arr += anyToStruct(tlvType, _))
                arr
            case _ =>
                throw new CodecException("not supported type in anyToStructArray, type=%s".format(v.getClass.getName));
        }
    }

    def fastConvertStruct(tlvType: TlvType, datamap: HashMapStringAny) {

        // 删除多余key
        var to_be_removed: ArrayBufferString = null
        for ((k, v) <- datamap) {
            if (!tlvType.structDef.keys.contains(k)) {
                if (to_be_removed == null) to_be_removed = ArrayBufferString()
                to_be_removed += k
            }
        }
        if (to_be_removed != null) {
            for (k <- to_be_removed) datamap.remove(k)
        }

        for (f <- tlvType.structDef.fields) {

            val key = f.name

            if (!datamap.contains(key)) {
                val allKeys = tlvType.structDef.keys
                val missedKeys = allKeys.filter(!datamap.contains(_))
                throw new CodecException("struct_not_valid, struct names= " + allKeys.mkString(",") + ", missed keys=" + missedKeys.mkString(","));
            }

            var v = datamap.getOrElse(key, null)
            if (v == null) {
                f.cls match {
                    case CLS_STRING | CLS_SYSTEMSTRING | CLS_VSTRING =>
                        datamap.put(key, "")
                    case CLS_INT =>
                        datamap.put(key, 0)
                    case CLS_LONG =>
                        datamap.put(key, 0L)
                    case CLS_DOUBLE =>
                        datamap.put(key, 0.0)
                }
            } else {
                f.cls match {
                    case CLS_STRING | CLS_SYSTEMSTRING | CLS_VSTRING =>
                        if (!v.isInstanceOf[String])
                            datamap.put(key, TypeSafe.anyToString(v))
                    case CLS_INT =>
                        if (!v.isInstanceOf[Int])
                            datamap.put(key, TypeSafe.anyToInt(v))
                    case CLS_LONG =>
                        if (!v.isInstanceOf[Long])
                            datamap.put(key, TypeSafe.anyToLong(v))
                    case CLS_DOUBLE =>
                        if (!v.isInstanceOf[Double])
                            datamap.put(key, TypeSafe.anyToDouble(v))
                }
            }

        }

    }

    def anyToObjectArray(tlvType: TlvType, value: Any, needEncode: Boolean): Tuple2[ArrayBufferMap, Int] = {

        value match {

            case list: ArrayBuffer[_] =>

                val arr = new ArrayBufferMap()
                var errorCode = 0

                for (v <- list) {
                    val (m, ec) = anyToObject(tlvType, v, needEncode)
                    arr += m
                    if (ec != 0 && errorCode == 0) errorCode = ec
                }

                (arr, errorCode)

            case list: Array[_] =>

                val arr = new ArrayBufferMap()
                var errorCode = 0

                for (v <- list) {
                    val (m, ec) = anyToObject(tlvType, v, needEncode)
                    arr += m
                    if (ec != 0 && errorCode == 0) errorCode = ec
                }

                (arr, errorCode)

            case _ =>
                throw new CodecException("not supported type in anyToObjectArray, type=%s".format(value.getClass.getName));
        }
    }

    // 编码，解码，内存中过滤都会调用此函数
    // 此函数 实现  1) 默认值设置  2) 校验  3) 编码转换功能
    def validate(keyMap: HashMapStringString, fieldMap: HashMap[String, FieldInfo],
                 dataMap: HashMap[String, Any], needEncode: Boolean): Int = {

        if (fieldMap == null || fieldMap.size == 0) return 0

        var errorCode = 0

        for ((key, fieldInfo) <- fieldMap) { // 只对做标记的field进行处理

            var typeName = keyMap.getOrElse(key, null)
            val tlvType = typeNameToCodeMap.getOrElse(typeName, null)

            // set default value
            var value = dataMap.getOrElse(key, null)
            if (value == null) {
                if (fieldInfo != null && fieldInfo.defaultValue != null) {
                    dataMap.put(key, safeConvertValue(fieldInfo.defaultValue, tlvType.cls))
                }
            } else {

                // 给结构体属性设默认值，不包括对象，对象会在递归调用中设置默认值
                tlvType.cls match {
                    case CLS_STRUCT =>
                        if (value.isInstanceOf[HashMapStringAny]) {
                            val m = value.asInstanceOf[HashMapStringAny]
                            setStructDefault(m, tlvType)
                        }
                    case CLS_STRUCTARRAY =>
                        if (value.isInstanceOf[ArrayBufferMap]) {
                            val lm = value.asInstanceOf[ArrayBufferMap]
                            for (m <- lm)
                                setStructDefault(m, tlvType)
                        }
                    case _ =>
                }
            }

            value = dataMap.getOrElse(key, null)

            // 不管什么类型，都要做这个校验, 非空检验在这里做
            val (v, ec) = validateValue(value, fieldInfo, needEncode)
            if (ec != 0 && errorCode == 0) errorCode = ec

            if (v != null) {

                if (v != value) {
                    value = safeConvertValue(v, tlvType.cls)
                    dataMap.put(key, value)
                }

                // 复杂类型：数组，结构体 ， 不包括对象，对象会在递归调用中得到校验
                tlvType.cls match {
                    case CLS_STRINGARRAY =>
                        val l = anyToStringArray(value)
                        for (i <- 0 until l.size) {
                            val (v, ec) = validateValue(l(i), tlvType.itemType.fieldInfo, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                            l(i) = TypeSafe.anyToString(v)
                        }
                        dataMap.put(key, l)
                    case CLS_INTARRAY =>
                        val l = anyToIntArray(value)
                        for (i <- 0 until l.size) {
                            val (v, ec) = validateValue(l(i), tlvType.itemType.fieldInfo, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                            l(i) = TypeSafe.anyToInt(v)
                        }
                        dataMap.put(key, l)
                    case CLS_LONGARRAY =>
                        val l = anyToLongArray(value)
                        for (i <- 0 until l.size) {
                            val (v, ec) = validateValue(l(i), tlvType.itemType.fieldInfo, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                            l(i) = TypeSafe.anyToLong(v)
                        }
                        dataMap.put(key, l)
                    case CLS_DOUBLEARRAY =>
                        val l = anyToDoubleArray(value)
                        for (i <- 0 until l.size) {
                            val (v, ec) = validateValue(l(i), tlvType.itemType.fieldInfo, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                            l(i) = TypeSafe.anyToDouble(v)
                        }
                        dataMap.put(key, l)
                    case CLS_STRUCT =>
                        val m = anyToStruct(tlvType, value)
                        val ec = validateStruct(m, tlvType, needEncode)
                        if (ec != 0 && errorCode == 0) errorCode = ec
                        dataMap.put(key, m)
                    case CLS_STRUCTARRAY =>
                        val l = anyToStructArray(tlvType, value)
                        for (i <- 0 until l.size) {
                            val ec = validateStruct(l(i), tlvType.itemType, needEncode)
                            if (ec != 0 && errorCode == 0) errorCode = ec
                        }
                        dataMap.put(key, l)
                    case _ =>
                }
            }

        }

        errorCode
    }

    def validateValue(v: Any, fieldInfo: FieldInfo, needEncode: Boolean): Tuple2[Any, Int] = {
        if (fieldInfo == null) return (v, 0)
        var errorCode = 0
        var b = v
        if (fieldInfo.validator != null) {
            val ret = fieldInfo.validator.validate(v)
            if (ret != 0) {
                log.error("validate_value_error, value=" + v + ", ret=" + ret)
                if (errorCode == 0) errorCode = ret
            }
        }
        if (needEncode && fieldInfo.encoder != null) {
            b = fieldInfo.encoder.encode(v)
        }
        (b, errorCode)
    }

    def validateStruct(map: HashMapStringAny, tlvType: TlvType, needEncode: Boolean): Int = {

        var errorCode = 0
        for (f <- tlvType.structDef.fields) {
            val fieldInfo = f.fieldInfo
            if (fieldInfo != null) {
                val key = f.name
                val value = map.getOrElse(key, null)
                if (fieldInfo.validator != null) {
                    val ret = fieldInfo.validator.validate(value)
                    if (ret != 0) {
                        log.error("validate_map_error, key=" + key + ", value=" + value + ", ret=" + ret)
                        if (errorCode == 0) errorCode = ret
                    }
                }
                if (needEncode && fieldInfo.encoder != null) {
                    val v = fieldInfo.encoder.encode(value)
                    map.put(key, safeConvertValue(v, f.cls))
                }
            }
        }
        errorCode
    }

    def setStructDefault(map: HashMapStringAny, tlvType: TlvType): Unit = {
        for (f <- tlvType.structDef.fields) {
            val fieldInfo = f.fieldInfo
            if (fieldInfo != null && fieldInfo.defaultValue != null) {
                val key = f.name
                val value = map.getOrElse(key, null)
                if (value == null ) {
                    map.put(key, safeConvertValue(fieldInfo.defaultValue, f.cls))
                }
            }
        }
    }

    def safeConvertValue(v: Any, cls: Int): Any = {
        cls match {
            case CLS_STRING | CLS_SYSTEMSTRING =>
                TypeSafe.anyToString(v)
            case CLS_INT =>
                TypeSafe.anyToInt(v)
            case CLS_LONG =>
                TypeSafe.anyToLong(v)
            case CLS_DOUBLE =>
                TypeSafe.anyToDouble(v)
            case _ =>
                v
        }
    }

}

class TlvCodecs(val dir: String) extends Logging {

    val codecs_id = new HashMap[Int, TlvCodec]()
    val codecs_names = new HashMap[String, TlvCodec]()
    val version_map = new HashMap[Int, Int]()

    init;

    def init() {

        val xmls = new File(dir).listFiles.map(_.getPath).filter(name => name.endsWith(".xml"))
        val xmls_sub = new File(dir).listFiles.filter(_.isDirectory).flatMap(_.listFiles).map(_.getPath).filter(name => name.endsWith(".xml"))

        val nameset = new HashSet[String]()
        for (f <- xmls) {
            val file = new File(f)
            nameset.add(file.getName)
        }
        for (f <- xmls_sub) {
            val file = new File(f)
            if (nameset.contains(file.getName))
                throw new RuntimeException("avenue config filename duplicated, file=" + f)
        }

        val allxmls = new ArrayBufferString()
        xmls.foreach(allxmls += _)
        xmls_sub.foreach(allxmls += _)

        for (f <- allxmls) {

            try {
                val cfgXml = XML.load(f)

                val id = (cfgXml \ "@id").toString.toInt
                val name = (cfgXml \ "@name").toString.toLowerCase

                val codec = new TlvCodec(f)

                if (codecs_id.getOrElse(id, null) != null) {
                    throw new RuntimeException("service id duplicated serviceId=" + id)
                }
                if (codecs_names.getOrElse(name, null) != null) {
                    throw new RuntimeException("service name duplicated name=" + name)
                }

                codecs_id.put(id, codec)
                codecs_names.put(name, codec)

                parseVersion(id, cfgXml)
            } catch {
                case e: Throwable =>
                    log.error("load xml failed, f=" + f)
                    throw e
            }
        }

    }

    def findTlvCodec(serviceId: Int): TlvCodec = {
        codecs_id.getOrElse(serviceId, null)
    }

    def serviceNameToId(service: String): Tuple2[Int, Int] = {
        val ss = service.toLowerCase.split("\\.")
        if (ss.length != 2) {
            return (0, 0)
        }
        val codec = codecs_names.getOrElse(ss(0), null)
        if (codec == null) {
            return (0, 0)
        }
        val msgId = codec.msgNameToId(ss(1))
        if (msgId == 0) {
            return (0, 0)
        }

        (codec.serviceId, msgId)
    }

    def serviceIdToName(serviceId: Int, msgId: Int): Tuple2[String, String] = {
        val codec = findTlvCodec(serviceId)
        if (codec == null) {
            return ("service" + serviceId.toString, "msg" + msgId.toString)
        }

        val msgName = codec.msgIdToName(msgId)
        if (msgName == null) {

            return (codec.serviceName, "msg" + msgId.toString)
        }

        (codec.serviceName, msgName)
    }

    def allServiceIds(): List[Int] = {
        codecs_id.keys.toList
    }

    def parseVersion(serviceId: Int, cfgXml: Node) {
        val version = (cfgXml \ "@version").text
        if (version != "")
            version_map.put(serviceId, version.toInt)
        else
            version_map.put(serviceId, 1)
    }

    def version(serviceId: Int) = {
        version_map.getOrElse(serviceId, 1)
    }
}

