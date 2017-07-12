package scalabpe.core

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

class CodecException(val msg: String) extends RuntimeException(msg)

object AvenueCodec {

    val STANDARD_HEADLEN_V1 = 44
    val STANDARD_HEADLEN_V2 = 28

    val TYPE_REQUEST = 0xA1
    val TYPE_RESPONSE = 0xA2

    val FORMAT_TLV = 0

    val ENCODING_GBK = 0
    val ENCODING_UTF8 = 1

    val MUSTREACH_NO = 0
    val MUSTREACH_YES = 1

    val ZERO = 0.toByte
    val ROUTE_FLAG = 0x0F.toByte
    val MASK = 0xff
    val EMPTY_SIGNATURE = new Array[Byte](16)

    val ACK_CODE = 100

    var encrypt_f: (ChannelBuffer, String) => ChannelBuffer = null
    var decrypt_f: (ChannelBuffer, String) => ChannelBuffer = null

    def parseEncoding(s: String): Int = {
        s.toLowerCase match {
            case "utf8" | "utf-8" => ENCODING_UTF8
            case _                => ENCODING_GBK
        }
    }
    def toEncoding(enc: Int): String = {
        enc match {
            case 0 => "gb18030";
            case _ => "utf-8"
        }
    }

}

class AvenueCodec {

    import AvenueCodec._

    def decode(req: ChannelBuffer, key: String = ""): AvenueData = {

        val length = req.writerIndex

        val flag = req.readByte() & MASK
        val headLen0 = req.readByte() & MASK
        val version = req.readByte() & MASK
        req.readByte() // ignore the field

        if (flag != TYPE_REQUEST && flag != TYPE_RESPONSE) {
            throw new CodecException("package_type_error")
        }
        if (version != 1 && version != 2) {
            throw new CodecException("package_version_error")
        }

        val standardLen = if (version == 1) STANDARD_HEADLEN_V1 else STANDARD_HEADLEN_V2
        val headLen = if (version == 2) headLen0 * 4 else headLen0

        if (length < standardLen) {
            throw new CodecException("package_size_error")
        }
        if (headLen < standardLen || headLen > length) {
            throw new CodecException("package_headlen_error, headLen=" + headLen + ",length=" + length)
        }

        val packLen = req.readInt()
        if (packLen != length) {
            throw new CodecException("package_packlen_error")
        }

        val serviceId = req.readInt()
        if (serviceId < 0) {
            throw new CodecException("package_serviceid_error")
        }

        val msgId = req.readInt()
        if (msgId < 0) {
            throw new CodecException("package_msgid_error")
        }
        if (msgId == 0 && serviceId != 0) {
            throw new CodecException("package_msgid_error")
        }
        if (msgId != 0 && serviceId == 0) {
            throw new CodecException("package_msgid_error")
        }

        if (serviceId == 0 && msgId == 0) {
            if (length != standardLen) {
                throw new CodecException("package_ping_size_error")
            }
        }

        val sequence = req.readInt()

        req.readByte() // ignore the field
        val mustReach = req.readByte()
        val format = req.readByte()
        val encoding = req.readByte()

        if (mustReach != MUSTREACH_NO && mustReach != MUSTREACH_YES) {
            throw new CodecException("package_mustreach_error")
        }

        if (format != FORMAT_TLV) {
            throw new CodecException("package_format_error")
        }

        if (encoding != ENCODING_GBK && encoding != ENCODING_UTF8) {
            throw new CodecException("package_encoding_error")
        }

        val code = req.readInt()

        if (version == 1) {
            req.readerIndex(req.readerIndex + 16)
        }

        val xheadlen = headLen - standardLen
        val xhead = ChannelBuffers.dynamicBuffer(xheadlen + 12)
        req.readBytes(xhead, xheadlen)
        val body0 = req.readSlice(packLen - headLen)

        val body = get_decrypted_body(body0, key)

        val r = new AvenueData(
            flag, version,
            serviceId, msgId, sequence,
            mustReach, encoding,
            if (flag == TYPE_REQUEST) 0 else code,
            xhead, body)

        r
    }

    def encode(res: AvenueData, key: String = ""): ChannelBuffer = {

        val body = get_encrypted_body(res.body, key)

        val standardLen = if (res.version == 1) STANDARD_HEADLEN_V1 else STANDARD_HEADLEN_V2

        val headLen0 = standardLen + res.xhead.writerIndex
        if (res.version == 2) {
            if ((headLen0 % 4) != 0) {
                throw new CodecException("package_xhead_padding_error")
            }
        }

        val headLen = if (res.version == 2) headLen0 / 4 else headLen0

        val packLen = standardLen + res.xhead.writerIndex + body.writerIndex

        val b = ChannelBuffers.buffer(standardLen)

        b.writeByte(res.flag.toByte) // type
        b.writeByte(headLen.toByte) // headLen
        b.writeByte(res.version.toByte) // version
        b.writeByte(ROUTE_FLAG) // route
        b.writeInt(packLen) // packLen
        b.writeInt(res.serviceId) // serviceId
        b.writeInt(res.msgId) // msgId
        b.writeInt(res.sequence) // sequence
        b.writeByte(ZERO) // context
        b.writeByte(res.mustReach.toByte) // mustReach
        b.writeByte(ZERO) // format
        b.writeByte(res.encoding.toByte) // encoding

        val code = if (res.flag == TYPE_REQUEST) 0 else res.code
        b.writeInt(code) // code

        if (res.version == 1)
            b.writeBytes(EMPTY_SIGNATURE) // signature

        ChannelBuffers.wrappedBuffer(b, res.xhead, body)
    }
    
    def get_decrypted_body(body0: ChannelBuffer, key: String = ""): ChannelBuffer = {
        if (key != null && key != "" && AvenueCodec.decrypt_f != null && body0 != null && body0.writerIndex > 0) {
            AvenueCodec.decrypt_f(body0, key)
        } else {
            body0
        }
    }

    def get_encrypted_body(body0: ChannelBuffer, key: String = ""): ChannelBuffer = {
        if (key != null && key != "" && AvenueCodec.encrypt_f != null && body0 != null && body0.writerIndex > 0) {
            AvenueCodec.encrypt_f(body0, key)
        } else {
            body0
        }
    }

}
