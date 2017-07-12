package scalabpe.core

import java.util.regex.Pattern

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.commons.lang.StringUtils

/*

validator定义为：

1、	validator系列标签可以出现在<type>内 或者请求/响应中的<field>内 或 struct里的 <field>内, 请求响应中的配置优先于type上的配置
2、	validator系列标签有3个：validator 、validatorParam、returnCode分别表示验证名、验证参数、验证失败后的返回码
3、	若请求字段校验失败，直接返回错误码。若响应字段校验失败，包体不变，包头code为修改为错误码。原响应包code!=0时不触发校验。

大类	validator	validatorParam	    参数说明	    returnCode	    实现说明

必填	Required    不需要	            不需要	        默认-10242400	用于判断必填，其中空字符串算做有值
正则类	Regex	    配置为正则表达式	是否符合正则	默认-10242400	最基础的validator
        Email	    不需要	            不需要	        默认-10242400	通过正则内置实现，等价于正则参数：([0-9A-Za-z\\-_\\.]+)@([a-zA-Z0-9_-])+(.[a-zA-Z0-9_-])+
        Url	        不需要	            不需要	        默认-10242400	通过正则内置实现
范围类 	NumberRange	数字1,数字2	        左闭由闭区间	默认-10242400	用于判断整数范围
        LengthRange	数字1,数字2	        左闭由闭区间	默认-10242400	用于判断字符串长度范围
        TimeRange	字符串1,字符串2	    左闭由闭区间	默认-10242400	用于判断时间范围 示例：2011-1-2 13:00:05.231,2018-2-3 15:00:00.345
集合类 	NumberSet	A|b|C|d|e|…	                     默认-10242400	 用于整数枚举类型，示例 0|1
        Regex	    A|b|C|d|e|…		                 默认-10242400 	完全同正则validator，普通正则实现 
 
*/

object Validator extends Logging {

    val cache = new HashMap[String, Validator]()
    val emailRegex = """^[A-Za-z0-9_.-]+@[a-zA-Z0-9_-]+[.][a-zA-Z0-9_-]+$"""
    val urlRegex = """^https?://[^/]+/.*$"""

    def getValidator(cls: String, param: String, returnCode: String): Validator = {
        if (cls == null || cls == "") return null
        val rc = if (returnCode == null || returnCode == "") -10242400 else returnCode.toInt

        val key = cls.toLowerCase + ":param=" + param + ":rc=" + rc
        val v0 = cache.getOrElse(key, null)
        if (v0 != null) return v0

        val v = cls.toLowerCase match {
            case "required" =>
                new RequireValidator(cls.toLowerCase, param, rc)
            case "regex" =>
                new RegexValidator(cls.toLowerCase, param, rc)
            case "email" =>
                new RegexValidator(cls.toLowerCase, emailRegex, rc)
            case "url" =>
                new RegexValidator(cls.toLowerCase, urlRegex, rc)
            case "numberrange" =>
                new NumberRangeValidator(cls.toLowerCase, param, rc)
            case "lengthrange" =>
                new LengthRangeValidator(cls.toLowerCase, param, rc)
            case "timerange" =>
                new TimeRangeValidator(cls.toLowerCase, param, rc)
            case "numberset" =>
                new ValueSetValidator(cls.toLowerCase, param, rc)
            case "valueset" =>
                new ValueSetValidator(cls.toLowerCase, param, rc)
            case _ =>
                log.error("unknown validator, cls=" + cls)
                throw new CodecException("unknown validator, cls=" + cls)
        }
        cache.put(key, v)
        return v
    }
}

trait Validator {
    val cls: String
    val param: String
    val returnCode: Int
    def validate(a: Any): Int
}

class RequireValidator(val cls: String, val param: String, val returnCode: Int) extends Validator with Logging {

    def validate(a: Any): Int = {
        if (a == null)
            return returnCode
        a match {
            case s: String =>
                if (s == "")
                    return returnCode
            case ls: ArrayBufferString =>
                if (ls.size == 0)
                    return returnCode
            case li: ArrayBufferInt =>
                if (li.size == 0)
                    return returnCode
            case ll: ArrayBufferLong =>
                if (ll.size == 0)
                    return returnCode
            case ld: ArrayBufferDouble =>
                if (ld.size == 0)
                    return returnCode
            case lm: ArrayBufferMap =>
                if (lm.size == 0)
                    return returnCode
            case _ =>
        }
        0
    }

}

class NumberRangeValidator(val cls: String, val param: String, val returnCode: Int) extends Validator with Logging {

    var min = Int.MinValue
    var max = Int.MaxValue

    init

    def init() {
        if (param == null || param == "")
            throw new CodecException("number range validator param is not valid, param is empty")
        val p = param.indexOf(",")
        if (p < 0)
            throw new CodecException("number range validator param is not valid, param=" + param)
        val v1 = param.substring(0, p)
        val v2 = param.substring(p + 1)
        if (v1 != "")
            min = v1.toInt
        if (v2 != "")
            max = v2.toInt
        if (max < min)
            throw new CodecException("number range validator param is not valid, param=" + param)
    }

    def validate(a: Any): Int = {
        a match {
            case i: Int =>
                if (i < min || i > max)
                    return returnCode
            case l: Long =>
                if (l < min || l > max)
                    return returnCode
            case d: Double =>
                if (d < min || d > max)
                    return returnCode
            case s: String =>
                if (s == null || s == "")
                    return returnCode
                try {
                    val i = s.toInt
                    if (i < min || i > max)
                        return returnCode
                } catch {
                    case e: Throwable =>
                        return returnCode
                }
            case _ =>
                return returnCode
        }
        0
    }

}

class LengthRangeValidator(val cls: String, val param: String, val returnCode: Int) extends Validator with Logging {

    var min = Int.MinValue
    var max = Int.MaxValue

    init

    def init() {
        if (param == null || param == "")
            throw new CodecException("length range validator param is not valid, param is empty")
        val p = param.indexOf(",")
        if (p < 0)
            throw new CodecException("length range validator param is not valid, param=" + param)
        val v1 = param.substring(0, p)
        val v2 = param.substring(p + 1)
        if (v1 != "")
            min = v1.toInt
        if (v2 != "")
            max = v2.toInt
        if (max < min)
            throw new CodecException("length range validator param is not valid, param=" + param)
    }

    def validate(a: Any): Int = {
        a match {
            case Int | Long | Double =>
                val len = a.toString.length
                if (len < min || len > max)
                    return returnCode
            case s: String =>
                if (s == null || s == "")
                    return returnCode
                val len = s.length
                if (len < min || len > max)
                    return returnCode
            case _ =>
                return returnCode
        }
        0
    }

}
class TimeRangeValidator(val cls: String, val param: String, val returnCode: Int) extends Validator with Logging {

    var min = "1970-01-01 00:00:00"
    var max = "2099-01-01 00:00:00"

    init

    def init() {
        if (param == null || param == "")
            throw new CodecException("time range validator param is not valid, param is empty")
        val p = param.indexOf(",")
        if (p < 0)
            throw new CodecException("time range validator param is not valid, param=" + param)
        val v1 = param.substring(0, p)
        val v2 = param.substring(p + 1)
        if (v1 != "")
            min = v1
        if (v2 != "")
            max = v2
        if (max < min)
            throw new CodecException("time range validator param is not valid, param=" + param)
    }

    def validate(a: Any): Int = {
        a match {
            case s: String =>
                if (s == null || s == "")
                    return returnCode
                if (s < min || s > max)
                    return returnCode
            case _ =>
                return returnCode
        }
        0
    }

}
class ValueSetValidator(val cls: String, val param: String, val returnCode: Int) extends Validator with Logging {

    val set = HashSet[String]()

    init

    def init() {
        if (param == null || param == "")
            throw new CodecException("value set validator param is not valid, param is empty")
        val ss = param.split("\\|")
        for (s <- ss) set.add(s)
    }

    def validate(a: Any): Int = {
        a match {
            case s: String =>
                if (s == null || s == "")
                    return returnCode
                if (!set.contains(s))
                    return returnCode
            case Int | Long | Double =>
                if (!set.contains(a.toString))
                    return returnCode
            case _ =>
                return returnCode
        }
        0
    }

}
class RegexValidator(val cls: String, val param: String, val returnCode: Int) extends Validator with Logging {

    var p: Pattern = _

    init

    def init() {
        if (param == null || param == "")
            throw new CodecException("regex validator param is not valid, param is empty")
        p = Pattern.compile(param)
    }

    def validate(a: Any): Int = {
        a match {
            case s: String =>
                if (s == null || s == "")
                    return returnCode
                if (!p.matcher(s).matches()) {
                    return returnCode
                }
            case Int | Long | Double =>
                if (!p.matcher(a.toString).matches()) {
                    return returnCode
                }
            case _ =>
                return returnCode
        }
        0
    }

}

/*
encoder定义为：

1、	encoder系列标签可以出现在<type>内 或者请求/响应中的<field>内, 或者struct里的<field>内, 请求响应中的配置优先于type上的配置
2、	encoder系列标签有2个：encoder、encoderParam分别表示编码名、编码参数
3、	encoder对请求、响应均有效
4、	拟实现的Encoder有：

encoder	            encoderParam	        参数说明	    实现说明

NormalEncoder	    A,b|c,d|<,&lt	        |是大分割符，逗号是小分隔符，代表将A转义为b,将c转义为d, |,\三个字符实现为关键字，要输入实际这三个字符使用\转义，比如\|   \,  \\
HtmlEncoder	        无	                    无	        基于NormalEncoder实现，等价于： &,&amp;|<,&lt;|>,&gt;|",&quot;|',&#x27;|/,&#x2f;
HtmlFilter	        无	                    无	        基于NormalEncoder实现，等价于： &,|<,|>,|",|',|/,|\\,
NocaseEncoder       无	                    无	        不区分大小写的NormalEncoder编码转换
AttackFilter        无	                    无	        基于NocaseEncoder,等价于： script,|exec,|select,|update,|delete,|insert,|create,|alter,|drop,|truncate,|&,|<,|>,|",|',|/,|\\,
 */

object Encoder extends Logging {

    val cache = new HashMap[String, Encoder]()
    val htmlParam = """&,&amp;|<,&lt;|>,&gt;|",&quot;|',&#x27;|/,&#x2f;"""
    val htmlFilterParam = """&,|<,|>,|",|',|/,|\\,"""
    val attackFilterParam = """script,|exec,|select,|update,|delete,|insert,|create,|alter,|drop,|truncate,|&,|<,|>,|",|',|/,|\\,"""

    def getEncoder(cls: String, param: String): Encoder = {
        if (cls == null || cls == "") return null

        val key = cls.toLowerCase + ":" + param
        val encoder = cache.getOrElse(key, null)
        if (encoder != null) return encoder

        val v = cls.toLowerCase match {
            case "escapeencoder" | "dummyencoder" =>
                new DummyEncoder(cls.toLowerCase, param)
            case "normalencoder" =>
                new NormalEncoder(cls.toLowerCase, param)
            case "htmlencoder" =>
                new HtmlEncoder(cls.toLowerCase, htmlParam)
            case "htmlfilter" =>
                new NormalEncoder(cls.toLowerCase, htmlFilterParam)
            case "nocaseencoder" =>
                new NoCaseEncoder(cls.toLowerCase, param)
            case "attackfilter" =>
                new NoCaseEncoder(cls.toLowerCase, attackFilterParam)
            case "maskencoder" =>
                new MaskEncoder(cls.toLowerCase, param)
            case _ =>
                log.error("unknown encoder, cls=" + cls)
                throw new CodecException("unknown encoder, cls=" + cls)
        }
        cache.put(key, v)
        return v
    }
}

trait Encoder {
    val cls: String
    val param: String
    def encode(a: Any): Any
}

class DummyEncoder(val cls: String, val param: String) extends Encoder with Logging {
    def encode(a: Any): Any = a
}

class NormalEncoder(val cls: String, val param: String) extends Encoder with Logging {

    val js = Array[String]("\\\\", "\\|", "\\,")
    val ks = Array[String](new String(Array[Byte](1)), new String(Array[Byte](2)), new String(Array[Byte](3)))
    val ls = Array[String]("\\", "|", ",")

    var p1: Array[String] = _
    var p2: Array[String] = _

    init

    def init() {
        if (param == null || param == "") return
        var t = j2k(param)
        val ss = t.split("\\|")
        val p1b = ArrayBufferString()
        val p2b = ArrayBufferString()

        for (s <- ss) {
            val p = s.indexOf(",")
            if (p <= 0) throw new CodecException("encoder param not valid, comma not found, param=" + param)
            val k = k2j(s.substring(0, p))
            val v = k2j(s.substring(p + 1))
            p1b += k
            p2b += v
        }
        p1 = p1b.toArray
        p2 = p2b.toArray
    }

    def j2k(s: String): String = {
        StringUtils.replaceEach(s, js, ks)
    }
    def k2j(s: String): String = {
        StringUtils.replaceEach(s, ks, ls)
    }

    def encodeString(s: String): String = {
        if (s == null) return s
        StringUtils.replaceEach(s, p1, p2)
    }

    def encodeInt(i: Int): Int = {
        try {
            encodeString(i.toString).toInt
        } catch {
            case e: Throwable =>
                log.error("encoder int error, i=" + i + ",cls=" + cls + ",param=" + param)
                i
        }
    }

    def encodeLong(l: Long): Long = {
        try {
            encodeString(l.toString).toLong
        } catch {
            case e: Throwable =>
                log.error("encoder int error, l=" + l + ",cls=" + cls + ",param=" + param)
                l
        }
    }

    def encode(a: Any): Any = {
        a match {
            case s: String => encodeString(s)
            case i: Int    => encodeInt(i)
            case l: Long   => encodeLong(l)
            case _         => a
        }
    }
}

class NoCaseEncoder(override val cls: String, override val param: String) extends NormalEncoder(cls, param) with Logging {

    override def init() {
        super.init()
        p1 = p1.map(_.toLowerCase)
    }

    override def encodeString(s: String): String = {
        if (s == null) return s
        val b = new StringBuilder(s)
        var i = 0
        while (i < p1.size) {
            replace(b, p1(i), p2(i))
            i += 1
        }
        b.toString
    }

    def replace(b: StringBuilder, s: String, d: String): Unit = {
        val slen = s.length
        val dlen = d.length
        var p = org.apache.commons.lang3.StringUtils.indexOfIgnoreCase(b, s, 0)
        while (p >= 0) {
            b.replace(p, p + slen, d)
            p = org.apache.commons.lang3.StringUtils.indexOfIgnoreCase(b, s, p + dlen)
        }
    }
}

// 注意: 
// NormalEncoder类在处理源和目标有重叠的情况，多次encoder会有问题
// HtmlEncoder对这个做了特殊处理，多次encoder不会引起问题
class HtmlEncoder(override val cls: String, override val param: String) extends NormalEncoder(cls, param) with Logging {

    var p3: Array[String] = _

    override def init() {
        super.init
        p3 = new Array[String](p2.size)
        for (i <- 0 until p3.size) {
            p3(i) = new String(Array[Byte]((i + 1).toByte))
        }
    }

    override def encodeString(s: String): String = {
        if (s == null) return s
        var t = StringUtils.replaceEach(s, p2, p3)
        t = super.encodeString(t)
        t = StringUtils.replaceEach(t, p3, p2)
        t
    }

}

class MaskEncoder(override val cls: String, override val param: String) extends Encoder with Logging {

    val commonRegex = """^common:([0-9]+):([0-9]+)$""".r
    var tp = if (param == null) "" else param.toLowerCase
    var start = 3
    var end = 4

    init

    def init() {
        tp match {
            case "phone" | "email" | "account" =>
            case commonRegex(s, e) =>
                tp = "common"
                start = s.toInt
                end = e.toInt
            case _ =>
                log.error("unknown encoder param, param=" + param)
                throw new CodecException("unknown encoder param, param=" + param)
        }
    }

    def encode(a: Any): Any = {
        a match {
            case s: String => encodeString(s)
            case i: Int    => i
            case l: Long   => l
            case d: Double => d
            case _         => a
        }
    }
    def encodeString(s: String): String = {
        if (s == null) return s

        tp match {
            case "phone"   => getCommonMaskForPhone(s)
            case "email"   => getCommonMaskForEmail(s)
            case "account" => getCommonMaskForCustomAccount(s)
            case "common"  => getCommonMask(s, start, end)
            case _         => s
        }
    }

    def getCommonMaskForPhone(phone: String): String = {
        getCommonMask(phone, 3, 4)
    }
    def getCommonMaskForEmail(s: String): String = {
        if (s == null) return null
        if (s.length <= 2) return s
        val p = s.indexOf("@")
        val name = s.substring(0, p)
        val suffix = s.substring(p)
        if (name.length <= 2) return s
        name.length match {
            case 3 => getCommonMask(name, 1, 1) + suffix
            case 4 => getCommonMask(name, 2, 1) + suffix
            case 5 => getCommonMask(name, 2, 2) + suffix
            case 6 => getCommonMask(name, 3, 2) + suffix
            case _ => getCommonMask(name, 3, 3) + suffix
        }
    }
    def getCommonMaskForCustomAccount(s: String): String = {
        if (s == null) return null
        if (s.length <= 2) return s
        s.length match {
            case 3 => getCommonMask(s, 1, 1)
            case 4 => getCommonMask(s, 2, 1)
            case 5 => getCommonMask(s, 2, 2)
            case 6 => getCommonMask(s, 3, 2)
            case _ => getCommonMask(s, 3, 3)
        }
    }
    def getCommonMask(s: String, start: Int, end: Int): String = {
        if (s == null) return null
        if (s.length <= start + end) return s
        return s.substring(0, start) + "****" + s.substring(s.length - end)
    }
}
