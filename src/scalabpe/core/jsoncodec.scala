package scalabpe.core
import java.io.StringWriter

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode

object JsonCodec {

    val mapper = new ObjectMapper()
    val jsonFactory = new JsonFactory()

    def parseJson(s: String): Any = {
        if (s == null || s == "") return null
        val node = mapper.readTree(s)
        if (node == null) return null
        node match {
            case _: ObjectNode =>
                return parseObject(node)
            case _: ArrayNode =>
                return parseArray(node)
        }
        null
    }

    def parseSimpleJson(s: String): HashMapStringAny = {
        parseObject(s)
    }

    def parseObject(s: String): HashMapStringAny = {
        if (s == null || s == "") return null
        try {
            val escapeStr = s.replaceAll("[\\r\\n]", " ")
            val node = mapper.readTree(escapeStr)
            if (node == null) return null
            parseObject(node)
        } catch {
            case _: Throwable => return null
        }
    }

    def parseObjectNotNull(s: String): HashMapStringAny = {
        val m = parseObject(s)
        if (m == null) return HashMapStringAny()
        m
    }

    def parseArray(s: String): ArrayBufferAny = {
        if (s == null || s == "") return null
        try {
            val escapeStr = s.replaceAll("[\\r\\n]", " ")
            val node = mapper.readTree(escapeStr)
            if (node == null) return null
            parseArray(node)
        } catch {
            case _: Throwable => return null
        }
    }

    def parseArrayNotNull(s: String): ArrayBufferAny = {
        val l = parseArray(s)
        if (l == null) return ArrayBufferAny()
        l
    }

    def parseArrayObject(s: String): ArrayBufferMap = {
        val a = parseArray(s)
        if (a == null) return null
        val am = new ArrayBufferMap()
        for (i <- a) {
            am += i.asInstanceOf[HashMapStringAny]
        }
        am
    }
    def parseArrayObjectNotNull(s: String): ArrayBufferMap = {
        val l = parseArrayObject(s)
        if (l == null) return ArrayBufferMap()
        l
    }

    def parseArrayString(s: String): ArrayBufferString = {
        val a = parseArray(s)
        if (a == null) return null
        val as = new ArrayBufferString()
        for (i <- a) {
            as += TypeSafe.anyToString(i)
        }
        as
    }
    def parseArrayStringNotNull(s: String): ArrayBufferString = {
        val l = parseArrayString(s)
        if (l == null) return ArrayBufferString()
        l
    }
    def parseArrayInt(s: String): ArrayBufferInt = {
        val a = parseArray(s)
        if (a == null) return null
        val ai = new ArrayBufferInt()
        for (i <- a) {
            ai += TypeSafe.anyToInt(i)
        }
        ai
    }
    def parseArrayIntNotNull(s: String): ArrayBufferInt = {
        val l = parseArrayInt(s)
        if (l == null) return ArrayBufferInt()
        l
    }
    def parseArrayLong(s: String): ArrayBufferLong = {
        val a = parseArray(s)
        if (a == null) return null
        val al = new ArrayBufferLong()
        for (i <- a) {
            al += TypeSafe.anyToLong(i)
        }
        al
    }
    def parseArrayLongNotNull(s: String): ArrayBufferLong = {
        val l = parseArrayLong(s)
        if (l == null) return ArrayBufferLong()
        l
    }
    def parseArrayDouble(s: String): ArrayBufferDouble = {
        val a = parseArray(s)
        if (a == null) return null
        val ad = new ArrayBufferDouble()
        for (i <- a) {
            ad += TypeSafe.anyToDouble(i)
        }
        ad
    }
    def parseArrayDoubleNotNull(s: String): ArrayBufferDouble = {
        val l = parseArrayDouble(s)
        if (l == null) return ArrayBufferDouble()
        l
    }

    def parseObject(node: JsonNode): HashMapStringAny = {
        val names = node.fieldNames
        val body = new HashMapStringAny()
        while (names.hasNext) {
            val name = names.next()
            val o = node.get(name)

            o match {
                case _: ArrayNode =>
                    val a = parseArray(o)
                    body.put(name, a)
                case _: ObjectNode =>
                    val m = parseObject(o)
                    body.put(name, m)
                case _: TextNode =>
                    body.put(name, o.asText)
                case _: IntNode =>
                    body.put(name, o.asText.toInt)
                case _: LongNode =>
                    body.put(name, o.asText.toLong)
                case _: DoubleNode =>
                    body.put(name, o.asText.toDouble)
                case _: BooleanNode =>
                    body.put(name, o.asText.toBoolean)
                case _: NullNode =>
                    body.put(name, null)
                case _ =>
                    body.put(name, o.asText)
            }

        }
        body
    }

    def parseArray(node: JsonNode): ArrayBufferAny = {
        val elems = node.elements
        val body = new ArrayBufferAny()
        while (elems.hasNext) {
            val o = elems.next()

            o match {
                case _: ArrayNode =>
                    val a = parseArray(o)
                    body += a
                case _: ObjectNode =>
                    val m = parseObject(o)
                    body += m
                case _: TextNode =>
                    body += o.asText
                case _: IntNode =>
                    body += o.asText.toInt
                case _: LongNode =>
                    body += o.asText.toLong
                case _: DoubleNode =>
                    body += o.asText.toDouble
                case _: BooleanNode =>
                    body += o.asText.toBoolean
                case _: NullNode =>
                    body += null
                case _ =>
                    body += o.asText
            }

        }
        body
    }

    def mkString(m: HashMapStringAny): String = {
        if (m == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        for ((key, value) <- m) {

            if (value == null) {
                jsonGenerator.writeNullField(key)
            } else {

                value match {
                    case s: String =>
                        jsonGenerator.writeStringField(key, s)
                    case i: Int =>
                        jsonGenerator.writeNumberField(key, i)
                    case l: Long =>
                        jsonGenerator.writeNumberField(key, l)
                    case d: Double =>
                        jsonGenerator.writeNumberField(key, d)
                    case d: Float =>
                        jsonGenerator.writeNumberField(key, d)
                    case b: Boolean =>
                        jsonGenerator.writeBooleanField(key, b)
                    case map: HashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case map: LinkedHashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferString =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferInt =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferLong =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferDouble =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferMap =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case _ =>
                        jsonGenerator.writeStringField(key, value.toString)
                }

            }

        }

        jsonGenerator.writeEndObject()
        jsonGenerator.close()

        writer.toString()

    }

    def mkString(m: LinkedHashMapStringAny): String = {
        if (m == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        for ((key, value) <- m) {

            if (value == null) {
                jsonGenerator.writeNullField(key)
            } else {

                value match {
                    case s: String =>
                        jsonGenerator.writeStringField(key, s)
                    case i: Int =>
                        jsonGenerator.writeNumberField(key, i)
                    case l: Long =>
                        jsonGenerator.writeNumberField(key, l)
                    case d: Double =>
                        jsonGenerator.writeNumberField(key, d)
                    case d: Float =>
                        jsonGenerator.writeNumberField(key, d)
                    case b: Boolean =>
                        jsonGenerator.writeBooleanField(key, b)
                    case map: HashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case map: LinkedHashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferString =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferInt =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferLong =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferDouble =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferMap =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case _ =>
                        jsonGenerator.writeStringField(key, value.toString)
                }

            }

        }

        jsonGenerator.writeEndObject()
        jsonGenerator.close()

        writer.toString()

    }

    def mkString(a: ArrayBufferAny): String = {

        if (a == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for (value <- a) {

            if (value == null) {
                jsonGenerator.writeNull()
            } else {

                value match {
                    case s: String =>
                        jsonGenerator.writeString(s)
                    case i: Int =>
                        jsonGenerator.writeNumber(i)
                    case l: Long =>
                        jsonGenerator.writeNumber(l)
                    case d: Double =>
                        jsonGenerator.writeNumber(d)
                    case d: Float =>
                        jsonGenerator.writeNumber(d)
                    case b: Boolean =>
                        jsonGenerator.writeBoolean(b)
                    case map: HashMapStringAny =>
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case map: LinkedHashMapStringAny =>
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferString =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferInt =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferLong =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferDouble =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferMap =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff: ArrayBufferAny =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case _ =>
                        jsonGenerator.writeString(value.toString)
                }

            }

        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

    def mkString(a: ArrayBufferMap): String = {

        if (a == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for (value <- a) {

            if (value == null) {
                jsonGenerator.writeNull()
            } else {
                val s = mkString(value)
                jsonGenerator.writeRawValue(s)
            }

        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

    def mkString(a: ArrayBufferInt): String = {

        if (a == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for (value <- a) {
            jsonGenerator.writeNumber(value)
        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

    def mkString(a: ArrayBufferLong): String = {

        if (a == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for (value <- a) {
            jsonGenerator.writeNumber(value)
        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

    def mkString(a: ArrayBufferDouble): String = {

        if (a == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for (value <- a) {
            jsonGenerator.writeNumber(value)
        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

    def mkString(a: ArrayBufferString): String = {

        if (a == null) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for (value <- a) {

            if (value == null) {
                jsonGenerator.writeNull()
            } else {
                jsonGenerator.writeString(value)
            }

        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

}

