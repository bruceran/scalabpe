package jvmdbbroker.core
import jvmdbbroker.core._
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._
import java.io.StringWriter
import scala.collection.mutable.LinkedHashMap

object JsonCodec {

    val mapper = new ObjectMapper()
    val jsonFactory = new JsonFactory()

    def parseSimpleJson(s:String):HashMapStringAny = {
        if( s == null || s == "" ) return null
        try {
            val valueTree = mapper.readTree(s)
            val names = valueTree.fieldNames
            val body = new HashMapStringAny()
            while( names.hasNext ) {
                val name = names.next()
                body.put(name, valueTree.get(name).asText)
            }
            body
        } catch {
            case _:Throwable => return null
        }
    }
    def parseJson(s:String):Any = {
        if( s == null || s == "" ) return null
        val node = mapper.readTree(s)
        if( node == null ) return null
        if( node.isInstanceOf[ObjectNode] ) 
            return parseObject(node)
        if( node.isInstanceOf[ArrayNode] ) 
            return parseArray(node)
        null
    }
    def parseObject(s:String):HashMapStringAny = {
        if( s == null || s == "" ) return null
        try {
            val escapeStr = s.replaceAll("[\\r\\n]"," ")
            val node = mapper.readTree(escapeStr)
            if( node == null ) return null
            parseObject(node)
        } catch {
            case _:Throwable => return null
        }
    }

    def parseObjectNotNull(s:String):HashMapStringAny = {
        val m = parseObjectNotNull(s)
        if( m == null ) return HashMapStringAny()
        m
    }

    def parseArray(s:String):ArrayBufferAny = {
        if( s == null || s == "" ) return null
        try {
            val escapeStr = s.replaceAll("[\\r\\n]"," ")
            val node = mapper.readTree(escapeStr)
            if( node == null ) return null
            parseArray(node)
        } catch {
            case _:Throwable => return null
        }
    }
    def parseArrayNotNull(s:String):ArrayBufferAny = {
        val l = parseArray(s)
        if( l == null ) return ArrayBufferAny()
        l
    }

    def parseArrayObject(s:String):ArrayBufferMap = {
        val a = parseArray(s)
        if( a == null ) return null
        val am = new ArrayBufferMap()
        for( i <- a ) {
            am += i.asInstanceOf[HashMapStringAny]
        }
        am
    }
    def parseArrayObjectNotNull(s:String):ArrayBufferMap = {
        val l = parseArrayObject(s)
        if( l == null ) return ArrayBufferMap()
        l
    }

    def parseArrayString(s:String):ArrayBufferString = {
        val a = parseArray(s)
        if( a == null ) return null
        val as = new ArrayBufferString()
        for( i <- a ) {
            as += i.asInstanceOf[String]
        }
        as
    }
    def parseArrayStringNotNull(s:String):ArrayBufferString = {
        val l = parseArrayString(s)
        if( l == null ) return ArrayBufferString()
        l
    }
    def parseArrayInt(s:String):ArrayBufferInt= {
        val a = parseArray(s)
        if( a == null ) return null
        val ai = new ArrayBufferInt()
        for( i <- a ) {
            ai += i.asInstanceOf[Int]
        }
        ai
    }
    def parseArrayIntNotNull(s:String):ArrayBufferInt = {
        val l = parseArrayInt(s)
        if( l == null ) return ArrayBufferInt()
        l
    }

    def parseObject(node:JsonNode):HashMapStringAny = {
        val names = node.fieldNames
        val body = new HashMapStringAny()
        while( names.hasNext ) {
            val name = names.next()
            val o = node.get(name)
            if( o.isInstanceOf[ArrayNode] ) {
                val a = parseArray(o)
                body.put(name, a)
            } else if( o.isInstanceOf[ObjectNode] ) {
                val m = parseObject(o)
                body.put(name, m)
            } else if( o.isInstanceOf[TextNode] ) {
                body.put(name, o.asText)
            } else if( o.isInstanceOf[IntNode] ) {
                body.put(name, o.asText.toInt)
            } else if( o.isInstanceOf[LongNode] ) {
                body.put(name, o.asText.toLong)
            } else if( o.isInstanceOf[DoubleNode] ) {
                body.put(name, o.asText.toDouble)
            } else if( o.isInstanceOf[BooleanNode] ) {
                body.put(name, o.asText.toBoolean)
            } else if( o.isInstanceOf[NullNode] ) {
                body.put(name, null)
            } else {
                body.put(name, o.asText)
            }
        }
        body
    }
    def parseArray(node:JsonNode):ArrayBufferAny = {
        val elems = node.elements
        val body = new ArrayBufferAny()
        while( elems.hasNext ) {
            val o = elems.next()
            if( o.isInstanceOf[ArrayNode] ) {
                val a = parseArray(o)
                body += a
            } else if( o.isInstanceOf[ObjectNode] ) {
                val m = parseObject(o)
                body += m
            } else if( o.isInstanceOf[TextNode] ) {
                body += o.asText
            } else if( o.isInstanceOf[IntNode] ) {
                body += o.asText.toInt
            } else if( o.isInstanceOf[LongNode] ) {
                body += o.asText.toLong
            } else if( o.isInstanceOf[DoubleNode] ) {
                body += o.asText.toDouble
            } else if( o.isInstanceOf[BooleanNode] ) {
                body += o.asText.toBoolean
            } else if( o.isInstanceOf[NullNode] ) {
                body += null
            } else {
                body += o.asText
            }
        }
        body
    }

    def mkString(m:HashMapStringAny): String = {
        if( m == null ) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        for( (key,value) <- m) {

            if( value == null ) {
                jsonGenerator.writeNullField(key)
            } else {

                value match {
                    case s:String =>
                        jsonGenerator.writeStringField(key,s)
                    case i:Int =>
                        jsonGenerator.writeNumberField(key,i)
                    case l:Long =>
                        jsonGenerator.writeNumberField(key,l)
                    case d:Double =>
                        jsonGenerator.writeNumberField(key,d)
                    case d:Float =>
                        jsonGenerator.writeNumberField(key,d)
                    case b:Boolean =>
                        jsonGenerator.writeBooleanField(key,b)
                    case map:HashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case map:LinkedHashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferString =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferInt =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferMap =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferAny =>
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

    def mkString(m:LinkedHashMapStringAny): String = {
        if( m == null ) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartObject()

        for( (key,value) <- m) {

            if( value == null ) {
                jsonGenerator.writeNullField(key)
            } else {

                value match {
                    case s:String =>
                        jsonGenerator.writeStringField(key,s)
                    case i:Int =>
                        jsonGenerator.writeNumberField(key,i)
                    case l:Long =>
                        jsonGenerator.writeNumberField(key,l)
                    case d:Double =>
                        jsonGenerator.writeNumberField(key,d)
                    case d:Float =>
                        jsonGenerator.writeNumberField(key,d)
                    case b:Boolean =>
                        jsonGenerator.writeBooleanField(key,b)
                    case map:HashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case map:LinkedHashMapStringAny =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferString =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferInt =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferMap =>
                        jsonGenerator.writeFieldName(key)
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferAny =>
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

    def mkString(a:ArrayBufferAny): String = {

        if( a == null ) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for( value <- a) {

            if( value == null ) {
                jsonGenerator.writeNull()
            } else {

                value match {
                    case s:String =>
                        jsonGenerator.writeString(s)
                    case i:Int =>
                        jsonGenerator.writeNumber(i)
                    case l:Long =>
                        jsonGenerator.writeNumber(l)
                    case d:Double =>
                        jsonGenerator.writeNumber(d)
                    case d:Float =>
                        jsonGenerator.writeNumber(d)
                    case b:Boolean =>
                        jsonGenerator.writeBoolean(b)
                    case map:HashMapStringAny =>
                        val s = mkString(map)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferString =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferInt =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferMap =>
                        val s = mkString(buff)
                        jsonGenerator.writeRawValue(s)
                    case buff:ArrayBufferAny =>
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

    def mkString(a:ArrayBufferMap): String = {

        if( a == null ) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for( value <- a) {

            if( value == null ) {
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

    def mkString(a:ArrayBufferInt): String = {

        if( a == null ) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for( value <- a) {
            jsonGenerator.writeNumber(value)
        }

        jsonGenerator.writeEndArray()
        jsonGenerator.close()

        writer.toString()
    }

    def mkString(a:ArrayBufferString): String = {

        if( a == null ) return null
        val writer = new StringWriter()
        val jsonGenerator = jsonFactory.createGenerator(writer)
        jsonGenerator.writeStartArray()

        for( value <- a) {

            if( value == null ) {
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

