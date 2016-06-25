package jvmdbbroker.core

import java.nio.ByteBuffer

class CodecException(val msg:String) extends RuntimeException(msg)

object AvenueCodec {

    val STANDARD_HEADLEN = 44

    val TYPE_REQUEST = 0xA1
    val TYPE_RESPONSE = 0xA2

    val ROUTE_FLAG = 0x0F

    val VERSION_1 = 1

    val FORMAT_TLV = 0
    val FORMAT_JSON = 1

    val ENCODING_GBK = 0
    val ENCODING_UTF8 = 1

    val ENCODING_STRINGS = Array("GB18030", "UTF-8")

    val MUSTREACH_NO = 0
    val MUSTREACH_YES = 1

    val ACK_CODE = 100

    val MASK = 0xff
    val EMPTY_SIGNATURE = new Array[Byte](16)

    val KEY_GS_INFO_FIRST = "gsInfoFirst"
    val KEY_GS_INFO_LAST = "gsInfoLast"

    val KEY_SOC_ID = "socId"
    val KEY_GS_INFOS = "gsInfos"
    val KEY_APP_ID = "appId"
    val KEY_AREA_ID = "areaId"
    val KEY_GROUP_ID = "groupId"
    val KEY_HOST_ID = "hostId"
    val KEY_SP_ID = "spId"
    val KEY_ENDPOINT_ID = "endpointId"
    val KEY_UNIQUE_ID = "uniqueId"
    val KEY_SPS_ID = "spsId"
    val KEY_HTTP_TYPE = "httpType"

    val CODE_SOC_ID = 1
    val CODE_GS_INFO = 2
    val CODE_APP_ID = 3
    val CODE_AREA_ID = 4
    val CODE_GROUP_ID = 5
    val CODE_HOST_ID = 6
    val CODE_SP_ID = 7
    val CODE_ENDPOINT_ID = 8
    val CODE_UNIQUE_ID = 9
    val CODE_SPS_ID = 11
    val CODE_HTTP_TYPE = 12

    var encrypt_f: (ByteBuffer,String)=>ByteBuffer = null
    var decrypt_f: (ByteBuffer,String)=>ByteBuffer = null

    def parseEncoding(s:String):Int = {

        s.toLowerCase match {
            case "utf8" | "utf-8" => ENCODING_UTF8
            case _ => ENCODING_GBK
        }
    }
}

class AvenueCodec {

  import AvenueCodec._

  def decode(req : ByteBuffer,key:String="") : AvenueData = {

        req.position(0)

        val length = req.remaining()

        if (length < STANDARD_HEADLEN) {
            throw new CodecException("package_size_error")
        }

        val flag = req.get() & MASK
        if (flag != TYPE_REQUEST && flag != TYPE_RESPONSE) {
            throw new CodecException("package_type_error")
        }

        val headLen = req.get() & MASK
        if (headLen < STANDARD_HEADLEN || headLen > length) {
            throw new CodecException("package_headlen_error, headLen="+headLen+",length="+length)
        }

        val version = req.get() & MASK
        if (version != VERSION_1) {
            throw new CodecException("package_version_error")
        }

        req.get()

        val packLen = req.getInt()
        if (packLen != length) {
            throw new CodecException("package_packlen_error")
        }

        val serviceId = req.getInt()
        if (serviceId < 0) {
            throw new CodecException("package_serviceid_error")
        }

        val msgId = req.getInt()
        if (msgId != 0) {
            if (serviceId == 0) {
                throw new CodecException("package_msgid_error")
            }
        }

        val sequence = req.getInt()

        req.get()

        val mustReach = req.get()
        val format = req.get()
        val encoding = req.get()

        if (mustReach != MUSTREACH_NO && mustReach != MUSTREACH_YES) {
            throw new CodecException("package_mustreach_error")
        }

        if (format != FORMAT_TLV && format != FORMAT_JSON) {
            throw new CodecException("package_format_error")
        }

        if (encoding != ENCODING_GBK && encoding != ENCODING_UTF8) {
            throw new CodecException("package_encoding_error")
        }

        val code = req.getInt()

        req.position(req.position() + 16)

        if (serviceId == 0 && msgId == 0) {
            if (length != STANDARD_HEADLEN) {
                throw new CodecException("package_ping_size_error")
            }
        }

        val xhead  = ByteBuffer.allocate(headLen - STANDARD_HEADLEN)
        req.get(xhead.array())
        xhead.position(0)

        var body = ByteBuffer.allocate(packLen - headLen)
        req.get(body.array())
        body.position(0)

        if( key != null && key != "" && AvenueCodec.decrypt_f != null) {
            body = AvenueCodec.decrypt_f(body,key)
         }

        val r = new AvenueData(
            flag,serviceId,msgId,sequence,
            mustReach,encoding,
            if (flag == TYPE_REQUEST) 0 else code,
            xhead,body )

        r
  }

  val ONE = 1.toByte
  val ZERO = 0.toByte

  def encode(res: AvenueData,key:String="") : ByteBuffer = {

        var body = res.body
        if( key != null && key != "" && AvenueCodec.encrypt_f != null ) {
            body = AvenueCodec.encrypt_f(body,key)
        }

        res.xhead.position(0)
        body.position(0)
        val headLen = STANDARD_HEADLEN+res.xhead.remaining()
        val packLen = headLen+body.remaining()

        val b = ByteBuffer.allocate(packLen)

        b.put(res.flag.toByte) // type
        b.put(headLen.toByte) // headLen
        b.put(ONE) // version
        b.put(ROUTE_FLAG.toByte) // route
        b.putInt(packLen) // packLen
        b.putInt(res.serviceId) // serviceId
        b.putInt(res.msgId) // msgId
        b.putInt(res.sequence) // sequence
        b.put(ZERO) // context
        b.put(ZERO) // mustReach
        b.put(ZERO) // format
        b.put(res.encoding.toByte) // encoding

        b.putInt(if (res.flag == TYPE_REQUEST) 0 else res.code) // code
        b.put(EMPTY_SIGNATURE) // signature

        b.put(res.xhead)
        b.put(body)
        b.flip()

        b
    }

}
