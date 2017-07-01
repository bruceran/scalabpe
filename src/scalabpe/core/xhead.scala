package scalabpe.core

import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.xml.Node
import scala.xml.XML

class XheadType(val code: Int, val name: String, val cls: String)

object Xhead {

    val KEY_SOC_ID = "socId"
    val KEY_ADDRS = "addrs"
    val KEY_UNIQUE_ID = "uniqueId"
    val KEY_SPS_ID = "spsId"
    val KEY_HTTP_TYPE = "httpType"

    val CODE_SOC_ID = 1
    val CODE_ADDRS = 2
    val CODE_UNIQUE_ID = 9
    val CODE_SPS_ID = 11
    val CODE_HTTP_TYPE = 12

    val KEY_FIRST_ADDR = "firstAddr"
    val KEY_LAST_ADDR = "lastAddr"

    val codeMap = HashMap[Int, XheadType]()
    val nameMap = HashMap[String, XheadType]()

    init()

    def init() {
        add(0, "signature", "string") // 签名，目前并没有使用到
        add(CODE_SOC_ID, KEY_SOC_ID, "string") // 客户端连接标识，serviceId=3时需特殊处理
        add(CODE_ADDRS, KEY_ADDRS, "addr") // 每个节点的IP和端口，格式特殊
        add(3, "appId", "int")
        add(4, "areaId", "int")
        add(5, "groupId", "int")
        add(6, "hostId", "int")
        add(7, "spId", "int")
        add(8, "endpointId", "string")
        add(CODE_UNIQUE_ID, KEY_UNIQUE_ID, "string")
        add(CODE_SPS_ID, KEY_SPS_ID, "string")
        add(CODE_HTTP_TYPE, KEY_HTTP_TYPE, "int")
        add(13, "logId", "string")
    }

    def add(code: Int, name: String, tp: String) {
        val v = new XheadType(code, name, tp)
        codeMap.put(code, v)
        nameMap.put(name, v)
    }

}

object TlvCodec4Xhead extends Logging {

    import Xhead._

    val SPS_ID_0 = "00000000000000000000000000000000"

    def decode(serviceId: Int, buff: ByteBuffer): HashMapStringAny = {
        try {
            val m = decodeInternal(serviceId, buff)
            return m
        } catch {
            case e: Exception =>
                log.error("xhead decode exception, e={}", e.getMessage)
                return new HashMapStringAny()
        }
    }

    def decodeInternal(serviceId: Int, buff: ByteBuffer): HashMapStringAny = {

        buff.position(0)

        val limit = buff.remaining

        val map = new HashMapStringAny()

        val isServiceId3 = (serviceId == 3)

        if (isServiceId3) {
            // only a "socId" field
            var len = 32
            if (limit < len) len = limit
            val value = new String(buff.array(), 0, len).trim()
            map.put(KEY_SOC_ID, value)
            return map
        }

        var break = false

        while (buff.position + 4 <= limit && !break) {

            val code = buff.getShort.toInt;
            val len: Int = buff.getShort & 0xffff;

            if (len < 4) {
                if (log.isDebugEnabled) {
                    log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
                    log.debug("xhead bytes=" + TlvCodec.toHexString(buff))
                }
                break = true
            }
            if (buff.position + len - 4 > limit) {
                if (log.isDebugEnabled) {
                    log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
                    log.debug("xhead bytes=" + TlvCodec.toHexString(buff))
                }
                break = true
            }

            if (!break) {

                val tp = codeMap.getOrElse(code, null)
                if (tp != null) {
                    tp.cls match {
                        case "string" =>
                            decodeString(buff, len, tp.name, map)
                        case "int" =>
                            decodeInt(buff, len, tp.name, map)
                        case "addr" =>
                            decodeAddr(buff, len, tp.name, map)
                    }
                } else {
                    var newposition = buff.position + aligned(len) - 4
                    if (newposition > buff.limit) newposition = buff.limit
                    buff.position(newposition)
                }
            }
        }

        map
    }

    def decodeInt(buff: ByteBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        if (len != 8) return
        val value = buff.getInt
        if (!map.contains(key))
            map.put(key, value)
    }
    def decodeString(buff: ByteBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        val value = new String(buff.array(), buff.position, len - 4)
        if (!map.contains(key))
            map.put(key, value)
        var newposition = buff.position + aligned(len) - 4
        if (newposition > buff.limit) newposition = buff.limit
        buff.position(newposition)
    }
    def decodeAddr(buff: ByteBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        if (len != 12) return
        val ips = new Array[Byte](4)
        buff.get(ips)
        val port = buff.getInt

        val ipstr = InetAddress.getByAddress(ips).getHostAddress()
        val value = ipstr + ":" + port

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

    def encode(serviceId: Int, map: HashMapStringAny): ByteBuffer = {
        try {
            val buff = encodeInternal(serviceId, map)
            buff
        } catch {
            case e: Exception =>
                log.error("xhead encode exception, e={}", e.getMessage)
                // throw new RuntimeException(e.getMessage)
                val buff = ByteBuffer.allocate(0)
                return buff
        }
    }

    def encodeInternal(serviceId: Int, map: HashMapStringAny): ByteBuffer = {

        val socId = map.getOrElse(KEY_SOC_ID, null)

        val isServiceId3 = (serviceId == 3)

        if (isServiceId3) {
            if (socId != null) {
                val buff = ByteBuffer.allocate(32)
                val bs = TypeSafe.anyToString(socId).getBytes()
                val len = if (bs.length > 32) 32 else bs.length;

                buff.put(bs, 0, len)
                for (i <- 0 until 32 - len) {
                    buff.put(0.toByte)
                }
                buff.flip()
                return buff
            } else {
                val buff = ByteBuffer.allocate(0)
                return buff
            }
        }

        val buff = ByteBuffer.allocate(1024-44) // TODO

        for ((k, v) <- map if v != null) {
            val tp = nameMap.getOrElse(k, null)
            if (tp != null) {
                tp.cls match {
                    case "string" =>
                        encodeString(buff, tp.code, v)
                    case "int" =>
                        encodeInt(buff, tp.code, v)
                    case "addr" =>
                        v match {
                            case infos: ArrayBufferString =>
                                for (info <- infos) {
                                    encodeAddr(buff, tp.code, info)
                                }
                            case _ =>
                                throw new CodecException("unknown addrs")
                        }
                }
            }
        }

        buff.flip()
        buff
    }

    def appendAddr(buff: ByteBuffer, addr: String, insertSpsId: Boolean = false): ByteBuffer = {

        var newlen = aligned(buff.limit) + 12

        if (insertSpsId)
            newlen += (aligned(addr.length) + 4 + aligned(SPS_ID_0.length) + 4)

        val newbuff = ByteBuffer.allocate(newlen)
        if (insertSpsId) {
            newbuff.position(aligned(newbuff.position))
            encodeString(newbuff, CODE_SPS_ID, SPS_ID_0)
            newbuff.position(aligned(newbuff.position))
            encodeString(newbuff, CODE_SOC_ID, addr)
        }

        newbuff.position(aligned(newbuff.position))
        newbuff.put(buff)
        newbuff.position(aligned(newbuff.position))
        encodeAddr(newbuff, CODE_ADDRS, addr)

        newbuff.flip
        newbuff
    }

    def encodeAddr(buff: ByteBuffer, code: Int, s: String): Unit = {

        if (buff.remaining < 12)
            throw new CodecException("xhead is too long")

        val ss = s.split(":")
        val ipBytes = InetAddress.getByName(ss(0)).getAddress()

        buff.putShort(code.toShort)
        buff.putShort(12.toShort)
        buff.put(ipBytes)
        buff.putInt(ss(1).toInt)
    }

    def encodeInt(buff: ByteBuffer, code: Int, v: Any): Unit = {
        if (buff.remaining < 8)
            throw new CodecException("xhead is too long")
        val value = TypeSafe.anyToInt(v)
        buff.putShort(code.toShort)
        buff.putShort(8.toShort)
        buff.putInt(value)
    }

    def encodeString(buff: ByteBuffer, code: Int, v: Any): Unit = {
        val value = TypeSafe.anyToString(v)
        if (value == null) return
        val bytes = value.getBytes() // don't support chinese
        val alignedLen = aligned(bytes.length + 4)
        if (buff.remaining < alignedLen)
            throw new CodecException("xhead is too long")
        buff.putShort(code.toShort)
        buff.putShort((bytes.length + 4).toShort)
        buff.put(bytes)
        buff.position(buff.position + alignedLen - bytes.length - 4)
    }

    def aligned(len: Int): Int = {
        if ((len & 0x03) != 0)
            ((len >> 2) + 1) << 2
        else
            len
    }
}

