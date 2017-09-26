package scalabpe.plugin

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import scalabpe.core.ArrayBufferAny
import scalabpe.core.ArrayBufferInt
import scalabpe.core.ArrayBufferLong
import scalabpe.core.ArrayBufferDouble
import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.HashMapStringAny
import scalabpe.core.JsonCodec
import scalabpe.core.Request
import scalabpe.core.TlvCodec
import scalabpe.core.TlvCodecs
import scalabpe.core.TlvType

object CacheLike {
    val MSGID_GET = 1
    val MSGID_SET = 2
    val MSGID_DELETE = 3
    val MSGID_GETANDDELETE = 4
    val MSGID_GETANDCAS = 5
    val MSGID_INCRBY = 6
    val MSGID_DECRBY = 7

    val MSGID_SGET = 10
    val MSGID_SADD = 11
    val MSGID_SREMOVE = 12

    // for local cache
    val MSGID_GETARRAY = 50
    val MSGID_GETALL = 51
}

trait CacheLike {

    val keyValueCountMap = HashMap[Int, Tuple2[Int, Int]]() // serviceId -> keyCount,valueCount
    val reqFieldsMap = HashMap[String, Tuple2[ArrayBuffer[String], ArrayBuffer[String]]]() // serviceId:msgId -> list of names
    val resFieldsMap = HashMap[String, Tuple3[ArrayBuffer[String], ArrayBuffer[String], ArrayBuffer[Int]]]() // serviceId:msgId -> list of names
    val resArrayFieldsMap = HashMap[String, Tuple2[ArrayBuffer[String], ArrayBuffer[String]]]() // serviceId:msgId -> list of names
    val nullFields = (ArrayBuffer[String](), ArrayBuffer[String]())
    val nullFields3 = (ArrayBuffer[String](), ArrayBuffer[String](), ArrayBuffer[Int]())

    def findReqFields(serviceId: Int, msgId: Int): Tuple2[ArrayBuffer[String], ArrayBuffer[String]] = {
        val s = reqFieldsMap.getOrElse(serviceId + ":" + msgId, nullFields)
        s
    }
    def findResFields(serviceId: Int, msgId: Int): Tuple3[ArrayBuffer[String], ArrayBuffer[String], ArrayBuffer[Int]] = {
        val s = resFieldsMap.getOrElse(serviceId + ":" + msgId, nullFields3)
        s
    }
    def findResArrayFields(serviceId: Int, msgId: Int): Tuple2[ArrayBuffer[String], ArrayBuffer[String]] = {
        val s = resArrayFieldsMap.getOrElse(serviceId + ":" + msgId, nullFields)
        s
    }
    def split(ts: String, splitter: String): ArrayBufferString = {
        val results = new ArrayBufferString()

        var s = ts
        var p = s.indexOf(splitter)
        while (p >= 0) {
            results += s.substring(0, p)
            s = s.substring(p + splitter.length)
            p = s.indexOf(splitter)
        }
        results += s

        results
    }

    def splitValue(s: String) = {
        split(s, "^_^")
    }

    def genValue(values: ArrayBuffer[Any]) = {
        val buff = new StringBuilder()
        var i = 0
        while (i < values.size) {
            if (i > 0)
                buff.append("^_^")
            if (values(i) != null)
                buff.append(values(i).toString)
            i += 1
        }

        buff.toString
    }
    def genValueOfInts(values: ArrayBuffer[Int]) = {
        val buff = new StringBuilder()
        var i = 0
        while (i < values.size) {
            if (i > 0)
                buff.append("^_^")
            buff.append(values(i))
            i += 1
        }

        buff.toString
    }
    def genValueOfStrs(values: ArrayBuffer[String]) = {
        val buff = new StringBuilder()
        var i = 0
        while (i < values.size) {
            if (i > 0)
                buff.append("^_^")
            if (values(i) != null)
                buff.append(values(i))
            i += 1
        }

        buff.toString
    }

    def parseKey(req: Request, keyFieldNames: ArrayBuffer[String]): String = {
        val keys = new ArrayBufferAny()
        for (key <- keyFieldNames) {

            val value = req.body.getOrElse(key, null)
            if (value == null) {
                return null
            }

            keys += value
        }

        val key = genKey(req.serviceId, keys)
        key
    }

    def genKey(serviceId: Int, values: ArrayBuffer[Any]): String = {
        "BPE" + serviceId + "_" + values.mkString("_")
    }
    def genKeyOfStrs(serviceId: Int, values: ArrayBuffer[String]): String = {
        "BPE" + serviceId + "_" + values.mkString("_")
    }
    def splitKeys(serviceId: Int, key: String): ArrayBufferString = {
        val prefix = "BPE" + serviceId + "_"
        split(key.substring(prefix.length), "_")
    }

    def genResponseBody(valueFieldNames: ArrayBuffer[String], valueFieldTypes: ArrayBuffer[Int], value: String): HashMapStringAny = {

        val valueArray = splitValue(value)

        val body = new HashMapStringAny()
        var i = 0
        while (i < valueFieldNames.size) {
            val key = valueFieldNames(i)
            val tp = valueFieldTypes(i)

            if (i < valueArray.length) {
                if (valueArray(i) != null && valueArray(i) != "") {
                    val v = valueArray(i)
                    var nv: Any = v
                    tp match {
                        case TlvType.CLS_STRUCT | TlvType.CLS_OBJECT =>
                            nv = JsonCodec.parseObject(v)
                        case TlvType.CLS_STRINGARRAY | TlvType.CLS_INTARRAY | TlvType.CLS_LONGARRAY | TlvType.CLS_DOUBLEARRAY | 
                             TlvType.CLS_STRUCTARRAY | TlvType.CLS_OBJECTARRAY  =>
                            nv = JsonCodec.parseArray(v)
                        case _ =>
                    }
                    body.put(key, nv)
                }
            }
            i += 1
        }

        body
    }

    def parseValue(req: Request, valueFieldNames: ArrayBuffer[String]): String = {
        val values = parseValues(req, valueFieldNames)
        if (values == null) {
            return null
        }
        val value = genValue(values)
        value
    }

    def parseValues(req: Request, valueFieldNames: ArrayBuffer[String]): ArrayBufferAny = {

        val values = new ArrayBufferAny()
        for (key <- valueFieldNames) {
            var value = req.body.getOrElse(key, "")
            value match {
                case v: HashMapStringAny  => value = JsonCodec.mkString(v)
                case v: ArrayBufferString => value = JsonCodec.mkString(v)
                case v: ArrayBufferInt    => value = JsonCodec.mkString(v)
                case v: ArrayBufferLong    => value = JsonCodec.mkString(v)
                case v: ArrayBufferDouble    => value = JsonCodec.mkString(v)
                case v: ArrayBufferMap    => value = JsonCodec.mkString(v)
                case _                    =>
            }
            values += value
        }

        values
    }

    def checkMsgKeyValueSize(serviceId: Int, msgId: Int) {

        val (keyCount, valueCount) = keyValueCountMap.getOrElse(serviceId, null)
        val (keyFieldNames, valueFieldNames) = findReqFields(serviceId, msgId)
        val (dummy1, valueFieldNamesRes, dummy2) = findResFields(serviceId, msgId)
        val (resKeyArrayNames, resValueArrayNames) = findResArrayFields(serviceId, msgId)

        var ok = true
        msgId match {

            case CacheLike.MSGID_GET =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 0 &&
                    valueFieldNamesRes.size == valueCount

            case CacheLike.MSGID_SET =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == valueCount

            case CacheLike.MSGID_DELETE =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 0

            case CacheLike.MSGID_GETANDDELETE =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 0 &&
                    valueFieldNamesRes.size == valueCount

            case CacheLike.MSGID_GETANDCAS =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == valueCount &&
                    valueFieldNamesRes.size == valueCount

            case CacheLike.MSGID_INCRBY =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 1 &&
                    valueCount == 1

            case CacheLike.MSGID_DECRBY =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 1 &&
                    valueCount == 1

            case CacheLike.MSGID_SGET =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 0 &&
                    valueFieldNamesRes.size == valueCount &&
                    valueCount == 1

            case CacheLike.MSGID_SADD | CacheLike.MSGID_SREMOVE =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == valueCount &&
                    valueCount == 1

            case CacheLike.MSGID_GETARRAY =>

                ok = keyFieldNames.size == keyCount &&
                    valueFieldNames.size == 0 &&
                    resKeyArrayNames.size == 0 &&
                    resValueArrayNames.size == valueCount

            case CacheLike.MSGID_GETALL =>

                ok = keyFieldNames.size == 0 &&
                    valueFieldNames.size == 0 &&
                    resKeyArrayNames.size == keyCount &&
                    resValueArrayNames.size == valueCount

            case _ =>

        }

        if (!ok) {
            throw new RuntimeException("cache define error, serviceId=%d,msgId=%d".format(serviceId, msgId))
        }
    }

    def initCacheTlv(serviceIds: String, codecs: TlvCodecs) {

        val serviceIdArray = serviceIds.split(",").map(_.toInt)
        for (serviceId <- serviceIdArray) {

            val codec = codecs.findTlvCodec(serviceId)

            if (codec == null) {
                throw new RuntimeException("serviceId not found, serviceId=" + serviceId)
            }

            if (codec != null) {

                val tlvTypes = codec.allTlvTypes()

                val keyCodes = tlvTypes.filter(a => a.code < 10000 && !TlvType.isArray(a.cls)).map(_.code).sorted
                val valueCodes = tlvTypes.filter(a => a.code > 10000 && a.code < 20000 && !TlvType.isArray(a.cls)).map(_.code).sorted // >20000 for parameters by name

                val keyNamesBuff = new ArrayBuffer[String]()
                val keyArrayNamesBuff = new ArrayBuffer[String]()
                for (code <- keyCodes) {
                    val t = codec.typeCodeToNameMap.getOrElse(code, TlvType.UNKNOWN)
                    if (TlvType.isArray(t.cls)) {
                        keyNamesBuff += t.itemType.name
                        keyNamesBuff += t.name
                        keyArrayNamesBuff += t.name
                    } else {
                        keyNamesBuff += t.name
                    }
                }
                val keyNames = keyNamesBuff
                val keyArrayNames = keyArrayNamesBuff
                //println(keyNames)
                //println(keyArrayNames)

                val valueNamesBuff = new ArrayBuffer[String]()
                val valueArrayNamesBuff = new ArrayBuffer[String]()
                for (code <- valueCodes) {
                    val t = codec.typeCodeToNameMap.getOrElse(code, TlvType.UNKNOWN)
                    if (TlvType.isArray(t.cls)) {
                        valueNamesBuff += t.itemType.name
                        valueNamesBuff += t.name
                        valueArrayNamesBuff += t.name
                    } else {
                        valueNamesBuff += t.name
                    }
                }
                val valueNames = valueNamesBuff
                val valueArrayNames = valueArrayNamesBuff
                //println(valueNames)
                //println(valueArrayNames)

                val tpl = (keyCodes.size, valueCodes.size)
                keyValueCountMap.put(serviceId, tpl)

                val msgIds = codec.allMsgIds()
                for (msgId <- msgIds) {
                    val reqNameMap = codec.msgTypeToKeyMapForReq.getOrElse(msgId, null)
                    val resNameMap = codec.msgTypeToKeyMapForRes.getOrElse(msgId, null)

                    val reqKeyNames = keyNames.filter(reqNameMap.contains).map(reqNameMap.getOrElse(_, null))
                    val reqValueNames = valueNames.filter(reqNameMap.contains).map(reqNameMap.getOrElse(_, null))
                    val resKeyNames = keyNames.filter(resNameMap.contains).map(resNameMap.getOrElse(_, null))
                    val resValueNames = valueNames.filter(resNameMap.contains).map(resNameMap.getOrElse(_, null))
                    val resValueTypes = valueNames.filter(resNameMap.contains).map(codec.typeNameToCodeMap.getOrElse(_, TlvType.UNKNOWN)).map(_.cls)

                    val resKeyArrayNames = keyArrayNames.filter(resNameMap.contains).map(resNameMap.getOrElse(_, null))
                    val resValueArrayNames = valueArrayNames.filter(resNameMap.contains).map(resNameMap.getOrElse(_, null))

                    reqFieldsMap.put(serviceId + ":" + msgId, (reqKeyNames, reqValueNames))
                    resFieldsMap.put(serviceId + ":" + msgId, (resKeyNames, resValueNames, resValueTypes))
                    resArrayFieldsMap.put(serviceId + ":" + msgId, (resKeyArrayNames, resValueArrayNames))

                    checkMsgKeyValueSize(serviceId, msgId)
                }

            }

        }

    }

}

