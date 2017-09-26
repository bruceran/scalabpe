package scalabpe.core

import java.net.InetAddress

import scala.collection.mutable.HashMap

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

class XheadType(val code: Int, val name: String, val cls: String)

object Xhead {

    val KEY_SOC_ID = "socId"
    val KEY_ADDRS = "addrs"
    val KEY_APP_ID = "appId"
    val KEY_AREA_ID = "areaId"
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
        add(CODE_SOC_ID, KEY_SOC_ID, "string") // 1 客户端连接标识，serviceId=3时需特殊处理
        add(CODE_ADDRS, KEY_ADDRS, "addr") // 2 每个节点的IP和端口，格式特殊
        add(3, KEY_APP_ID, "int")
        add(4, KEY_AREA_ID, "int")
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
    import TlvCodec._

    val SPS_ID_0 = "00000000000000000000000000000000"  // 32个字符

    def decode(serviceId: Int, buff: ChannelBuffer): HashMapStringAny = {
        try {
            return decodeInternal(serviceId, buff)
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
            val len = Math.min(limit, 32)
            val value = readString(buff, len, "utf-8").trim()
            map.put(KEY_SOC_ID, value)
            return map
        }

        var brk = false

        while (buff.readerIndex + 4 <= limit && !brk) {

            val code = buff.readShort.toInt;
            val len = buff.readShort & 0xffff;

            if (len < 4) {
                if (log.isDebugEnabled) {
                    log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
                    log.debug("xhead hexDump=" + hexDump(buff))
                }
                brk = true
            }
            if (buff.readerIndex + len - 4 > limit) {
                if (log.isDebugEnabled) {
                    log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
                    log.debug("xhead bytes=" + hexDump(buff))
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
                    skipRead(buff, aligned(len) - 4)
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
        if (len != 8) {
            throw new CodecException("invalid int length")
        }
        val value = buff.readInt
        if (!map.contains(key))
            map.put(key, value)
    }

    def decodeString(buff: ChannelBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        val value = readString(buff, len - 4,"utf-8")
        if (!map.contains(key))
            map.put(key, value)
    }

    def decodeAddr(buff: ChannelBuffer, len: Int, key: String, map: HashMapStringAny): Unit = {
        if (len != 12) {
            throw new CodecException("invalid addr length")
        }
        val ips = new Array[Byte](4)
        buff.readBytes(ips)
        val port = buff.readInt

        val ipstr = InetAddress.getByAddress(ips).getHostAddress()
        val value = ipstr + ":" + port

        val addrs = map.nls(key)
        addrs += value
        map.put(key, addrs)
    }

    def encode(serviceId: Int, map: HashMapStringAny, version: Int): ChannelBuffer = {
        try {
            return encodeInternal(serviceId, map, version)
        } catch {
            case e: Exception =>
                log.error("xhead encode exception, e={}", e.getMessage)
                return ChannelBuffers.buffer(0)
        }
    }

    def encodeInternal(serviceId: Int, map: HashMapStringAny, version: Int): ChannelBuffer = {

        val socId = map.getOrElse(KEY_SOC_ID, null)

        val isServiceId3 = (serviceId == 3)

        if (isServiceId3) {
            if (socId != null) {
                val buff = ChannelBuffers.buffer(32)
                val bs = TypeSafe.anyToString(socId).getBytes()
                val len = Math.min(bs.length,32)

                buff.writeBytes(bs, 0, len)
                for (i <- 0 until 32 - len) {
                    buff.writeByte(0.toByte)
                }
                return buff
            } else {
                return ChannelBuffers.buffer(0)
            }
        }

        val max = maxXheadLen(version)
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

    def encodeAddr(buff: ChannelBuffer, code: Int, s: String, max: Int): Unit = {
        if (max - buff.writerIndex < 12) {
            log.error("xhead is too long, addr cannot be encoded")
            return
        }

        val ss = s.split(":")
        val ipBytes = InetAddress.getByName(ss(0)).getAddress()

        buff.writeShort(code.toShort)
        buff.writeShort(12.toShort)
        buff.writeBytes(ipBytes)
        buff.writeInt(ss(1).toInt)
    }

    def encodeInt(buff: ChannelBuffer, code: Int, v: Any, max: Int): Unit = {
        if (max - buff.writerIndex < 8) {
            log.error("xhead is too long, Int " + code + " cannot be encoded")
            return
        }
        val value = TypeSafe.anyToInt(v)
        buff.writeShort(code.toShort)
        buff.writeShort(8.toShort)
        buff.writeInt(value)
    }

    def encodeString(buff: ChannelBuffer, code: Int, v: Any, max: Int): Unit = {
        val value = TypeSafe.anyToString(v)
        if (value == null) return
        val bytes = value.getBytes("utf-8")
        val alignedLen = aligned(bytes.length + 4)
        if (max - buff.writerIndex < alignedLen) {
            log.error("xhead is too long, String " + code + " cannot be encoded")
            return
        }
        buff.writeShort(code.toShort)
        buff.writeShort((bytes.length + 4).toShort)
        buff.writeBytes(bytes)
        writePad(buff)
    }

    def maxXheadLen(version: Int): Int = {
        if (version == 1) 256 - 44
        else 1024 - 28
    }

    def appendAddr(buff: ChannelBuffer, addr: String, insertSocId: Boolean, version: Int): Unit = {
        val max = maxXheadLen(version)
        encodeAddr(buff, CODE_ADDRS, addr, max)
        if (insertSocId) {
            encodeString(buff, CODE_SOC_ID, addr, max)
            encodeString(buff, CODE_SPS_ID, SPS_ID_0, max) // 预写入一个值，调用updateSpsId再更新为实际值
            val uuid = java.util.UUID.randomUUID().toString().replaceAll("-", "")
            encodeString(buff, CODE_UNIQUE_ID, uuid, max)
        }
    }

    // buff是一个完整的avenue包, 必须定位到扩展包头的开始再按code查找到CODE_SPS_ID
    def updateSpsId(buff: ChannelBuffer, spsId: String): Unit = {

        val version = buff.getByte(2).toInt
        val standardlen = if (version == 1) 44 else 28
        val headLen0 = buff.getByte(1) & 0xff;
        val headLen = if (version == 1) headLen0 else headLen0 * 4

        var start = standardlen
        while (start + 4 <= headLen) {
            val code = buff.getShort(start).toInt;
            val len = buff.getShort(start + 2) & 0xffff;
            if (code != CODE_SPS_ID) {
                start += aligned(len)
            } else {
                buff.setBytes(start + 4, spsId.getBytes("ISO-8859-1"))
                return
            }
        }

    }
}

