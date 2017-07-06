package scalabpe.core

import java.net.InetAddress

import scala.collection.mutable.HashMap

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

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
        add(0, "signature", "bytes") // 签名，目前并没有使用到, 要用的时候再加
        add(CODE_SOC_ID, KEY_SOC_ID, "string") // 客户端连接标识，serviceId=3时需特殊处理
        add(CODE_ADDRS, KEY_ADDRS, "addr") // 每个节点的IP和端口，格式特殊
        add(3, "appId", "int")
        add(4, "areaId", "int")
        add(5, "groupId", "int")
        add(6, "hostId", "int")
        add(7, "spId", "int")
        add(8, "endpointId", "string")
        add(CODE_UNIQUE_ID, KEY_UNIQUE_ID, "string") // 9
        add(CODE_SPS_ID, KEY_SPS_ID, "string") // 11 
        add(CODE_HTTP_TYPE, KEY_HTTP_TYPE, "int") // 12
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

    def decode(serviceId: Int, buff: ChannelBuffer): HashMapStringAny = {
        try {
            val m = decodeInternal(serviceId, buff)
            return m
        } catch {
            case e: Exception =>
                log.error("xhead decode exception, e={}", e.getMessage)
                return new HashMapStringAny()
        }
    }

    def decodeInternal(serviceId: Int, buff: ChannelBuffer): HashMapStringAny = {

        val limit = buff.writerIndex
        val map = new HashMapStringAny()

        val isServiceId3 = (serviceId == 3)

        if (isServiceId3) {
            // only a "socId" field
            var len = 32
            if (limit < len) len = limit
            val value = getString(buff, len).trim()
            map.put(KEY_SOC_ID, value)
            return map
        }

        var brk = false

        while (buff.readerIndex + 4 <= limit && !brk) {

            val code = buff.readShort.toInt;
            val len: Int = buff.readShort & 0xffff;

            if (len < 4) {
                if (log.isDebugEnabled) {
                    log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
                    log.debug("xhead hexDump=" + TlvCodec.hexDump(buff))
                }
                brk = true
            }
            if (buff.readerIndex + len - 4 > limit) {
                if (log.isDebugEnabled) {
                    log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
                    log.debug("xhead bytes=" + TlvCodec.hexDump(buff))
                }
                brk = true
            }

            if (!brk) {

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
                    var newposition = buff.readerIndex + aligned(len) - 4
                    if (newposition > buff.writerIndex) newposition = buff.writerIndex
                    buff.readerIndex(newposition)
                }
            }
        }

        val addrs = map.nls("addrs")
        if (addrs.size > 0) {
            map.put(KEY_FIRST_ADDR, addrs(0))
            map.put(KEY_LAST_ADDR, addrs(addrs.size - 1))
        }
        map
    }

    def decodeInt(buff: ChannelBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        if (len != 8) return
        val value = buff.readInt
        if (!map.contains(key))
            map.put(key, value)
    }
    def decodeString(buff: ChannelBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        val value = getString(buff, len - 4)
        if (!map.contains(key))
            map.put(key, value)
        var newposition = buff.readerIndex + aligned(len) - 4
        if (newposition > buff.writerIndex) newposition = buff.writerIndex
        buff.readerIndex(newposition)
    }
    def decodeAddr(buff: ChannelBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        if (len != 12) return
        val ips = new Array[Byte](4)
        buff.readBytes(ips)
        val port = buff.readInt

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

    def getString(buff: ChannelBuffer, len: Int): String = {
        val bytes = new Array[Byte](len)
        buff.getBytes(buff.readerIndex, bytes)
        new String(bytes, "UTF-8")
    }

    def encode(serviceId: Int, map: HashMapStringAny, version: Int): ChannelBuffer = {
        try {
            val buff = encodeInternal(serviceId, map, version)
            buff
        } catch {
            case e: Exception =>
                log.error("xhead encode exception, e={}", e.getMessage)
                val buff = ChannelBuffers.buffer(0)
                return buff
        }
    }

    def encodeInternal(serviceId: Int, map: HashMapStringAny, version: Int): ChannelBuffer = {

        val socId = map.getOrElse(KEY_SOC_ID, null)

        val isServiceId3 = (serviceId == 3)

        if (isServiceId3) {
            if (socId != null) {
                val buff = ChannelBuffers.buffer(32)
                val bs = TypeSafe.anyToString(socId).getBytes()
                val len = if (bs.length > 32) 32 else bs.length;

                buff.writeBytes(bs, 0, len)
                for (i <- 0 until 32 - len) {
                    buff.writeByte(0.toByte)
                }
                return buff
            } else {
                val buff = ChannelBuffers.buffer(0)
                return buff
            }
        }

        val max = if (version == 1) 256 - 44 else 1024 - 28
        val buff = ChannelBuffers.dynamicBuffer(128)

        for ((k, v) <- map if v != null) {
            val tp = nameMap.getOrElse(k, null)
            if (tp != null) {
                tp.cls match {
                    case "string" =>
                        encodeString(buff, tp.code, v, max)
                    case "int" =>
                        encodeInt(buff, tp.code, v, max)
                    case "addr" =>
                        v match {
                            case infos: ArrayBufferString =>
                                for (info <- infos) {
                                    encodeAddr(buff, tp.code, info, max)
                                }
                            case _ =>
                                throw new CodecException("unknown addrs")
                        }
                }
            }
        }

        buff
    }

    def appendAddr(buff: ChannelBuffer, addr: String, insertSpsId: Boolean, version: Int): ChannelBuffer = {

        var newlen = aligned(buff.writerIndex) + 12

        if (insertSpsId)
            newlen += (aligned(addr.length) + 4 + aligned(SPS_ID_0.length) + 4)

        val newbuff = ChannelBuffers.buffer(newlen)

        if (insertSpsId) {
            encodeString(newbuff, CODE_SPS_ID, SPS_ID_0, newlen)
            encodeString(newbuff, CODE_SOC_ID, addr, newlen)
        }

        newbuff.writeBytes(buff)
        TlvCodec.writePad(newbuff, aligned(newbuff.writerIndex))
        encodeAddr(newbuff, CODE_ADDRS, addr, newlen)

        newbuff
    }

    def encodeAddr(buff: ChannelBuffer, code: Int, s: String, max: Int): Unit = {

        if (max - buff.writerIndex < 12)
            throw new CodecException("xhead is too long")

        val ss = s.split(":")
        val ipBytes = InetAddress.getByName(ss(0)).getAddress()

        buff.writeShort(code.toShort)
        buff.writeShort(12.toShort)
        buff.writeBytes(ipBytes)
        buff.writeInt(ss(1).toInt)
    }

    def encodeInt(buff: ChannelBuffer, code: Int, v: Any, max: Int): Unit = {
        if (max - buff.writerIndex < 8)
            throw new CodecException("xhead is too long")
        val value = TypeSafe.anyToInt(v)
        buff.writeShort(code.toShort)
        buff.writeShort(8.toShort)
        buff.writeInt(value)
    }

    def encodeString(buff: ChannelBuffer, code: Int, v: Any, max: Int): Unit = {
        val value = TypeSafe.anyToString(v)
        if (value == null) return
        val bytes = value.getBytes() // don't support chinese
        val alignedLen = aligned(bytes.length + 4)
        if (max - buff.writerIndex < alignedLen)
            throw new CodecException("xhead is too long")
        buff.writeShort(code.toShort)
        buff.writeShort((bytes.length + 4).toShort)
        buff.writeBytes(bytes)
        TlvCodec.writePad(buff, buff.writerIndex + alignedLen - bytes.length - 4)
    }

    def aligned(len: Int): Int = {
        if ((len & 0x03) != 0)
            ((len >> 2) + 1) << 2
        else
            len
    }
}

