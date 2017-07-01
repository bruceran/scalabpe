package scalabpe.core

import java.nio.ByteBuffer

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

    var encrypt_f: (ByteBuffer, String) => ByteBuffer = null
    var decrypt_f: (ByteBuffer, String) => ByteBuffer = null

    def parseEncoding(s: String): Int = {
        s.toLowerCase match {
            case "utf8" | "utf-8" => ENCODING_UTF8
            case _                => ENCODING_GBK
        }
    }
    def toEncoding(enc: Int): String = {
        enc match {
            case 0 => "GB18030";
            case _ => "UTF-8"
        }
    }

}

class AvenueCodec {

    import AvenueCodec._

    def decode(req: ByteBuffer, key: String = ""): AvenueData = {

        req.position(0)

        val length = req.remaining()

        val flag = req.get() & MASK
        var headLen = req.get() & MASK
        val version = req.get() & MASK
        req.get() // ignore the field

        if (flag != TYPE_REQUEST && flag != TYPE_RESPONSE) {
            throw new CodecException("package_type_error")
        }
        if (version != 1 && version != 2) {
            throw new CodecException("package_version_error")
        }

        val standardLen = if (version == 1) STANDARD_HEADLEN_V1 else STANDARD_HEADLEN_V2
        if (version == 2) headLen *= 4

        if (length < standardLen) {
            throw new CodecException("package_size_error")
        }
        if (headLen < standardLen || headLen > length) {
            throw new CodecException("package_headlen_error, headLen=" + headLen + ",length=" + length)
        }

        val packLen = req.getInt()
        if (packLen != length) {
            throw new CodecException("package_packlen_error")
        }

        val serviceId = req.getInt()
        if (serviceId < 0) {
            throw new CodecException("package_serviceid_error")
        }

        val msgId = req.getInt()
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

        val sequence = req.getInt()

        req.get() // ignore the field
        val mustReach = req.get()
        val format = req.get()
        val encoding = req.get()

        if (mustReach != MUSTREACH_NO && mustReach != MUSTREACH_YES) {
            throw new CodecException("package_mustreach_error")
        }

        if (format != FORMAT_TLV) {
            throw new CodecException("package_format_error")
        }

        if (encoding != ENCODING_GBK && encoding != ENCODING_UTF8) {
            throw new CodecException("package_encoding_error")
        }

        val code = req.getInt()

        if (version == 1) {
            req.position(req.position() + 16)
        }

        val xhead = ByteBuffer.allocate(headLen - standardLen)
        req.get(xhead.array())
        xhead.position(0)

        var body = ByteBuffer.allocate(packLen - headLen)
        req.get(body.array())
        body.position(0)

        if (key != null && key != "" && AvenueCodec.decrypt_f != null && body != null && body.limit() > 0) {
            body = AvenueCodec.decrypt_f(body, key)
        }

        val r = new AvenueData(
            flag, version, serviceId, msgId, sequence,
            mustReach, encoding,
            if (flag == TYPE_REQUEST) 0 else code,
            xhead, body)

        r
    }

    def encode(res: AvenueData, key: String = ""): ByteBuffer = {

        var body = res.body
        if (key != null && key != "" && AvenueCodec.encrypt_f != null && body != null && body.limit() > 0) {
            body = AvenueCodec.encrypt_f(body, key)
        }

        res.xhead.position(0)
        body.position(0)

        val standardLen = if (res.version == 1) STANDARD_HEADLEN_V1 else STANDARD_HEADLEN_V2

        var headLen = standardLen + res.xhead.remaining()
        if (res.version == 2) {
            if ((headLen % 4) != 0) {
                throw new CodecException("package_xhead_padding_error")
            }
            headLen /= 4
        }
        val packLen = standardLen + res.xhead.remaining() + body.remaining()

        val b = ByteBuffer.allocate(packLen)

        b.put(res.flag.toByte) // type
        b.put(headLen.toByte) // headLen
        b.put(res.version.toByte) // version
        b.put(ROUTE_FLAG) // route
        b.putInt(packLen) // packLen
        b.putInt(res.serviceId) // serviceId
        b.putInt(res.msgId) // msgId
        b.putInt(res.sequence) // sequence
        b.put(ZERO) // context
        b.put(res.mustReach.toByte) // mustReach
        b.put(ZERO) // format
        b.put(res.encoding.toByte) // encoding

        b.putInt(if (res.flag == TYPE_REQUEST) 0 else res.code) // code

        if (res.version == 1)
            b.put(EMPTY_SIGNATURE) // signature

        b.put(res.xhead)
        b.put(body)
        b.flip()

        b
    }

}
