package scalabpe.plugin.cache

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.frame.FrameDecoder

import scalabpe.core.ArrayBufferMap
import scalabpe.core.ArrayBufferString
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.ResultCodes
import scalabpe.core.SelfCheckResult

object RedisType {
    val TYPE_UNKNOWN = -1
    val TYPE_ARRAYHASH = 1
    val TYPE_CONHASH = 2
    val TYPE_MASTERSLAVE = 3 // 这个是客户端自己实现的master/slave模式，不是redis服务端的主备模式
    val TYPE_CLUSTER = 4
}

trait RedisSocTrait {
    def send(req: Request, timeout: Int): Unit
    def close(): Unit
    def dump(): Unit
    def selfcheck(): ArrayBuffer[SelfCheckResult]
}

class RedisResponse(val ok: Boolean, val value: String, val arr: ArrayBuffer[RedisResponse] = null)

object RedisCodec {

    val MOVED_ERROR = -9999
    val ASK_ERROR = -9998

    val MSGID_PING = 1
    val MSGID_DEL = 2
    val MSGID_EXPIRE = 3
    val MSGID_EXPIREAT = 4
    val MSGID_EXISTS = 20
    val MSGID_CLUSTER_SLOTS = 21
    val MSGID_ASKING = 22

    val MSGID_SET = 100
    val MSGID_GETSET = 101
    val MSGID_INCRBY = 102
    val MSGID_DECRBY = 103
    val MSGID_GET = 120
    val MSGID_MGET = 121 // 不支持非master/slave的多节点部署

    val MSGID_SADD = 200
    val MSGID_SREM = 201
    val MSGID_SPOP = 202
    val MSGID_SDIFFSTORE = 203 // 不支持非master/slave的多节点部署
    val MSGID_SINTERSTORE = 204 // 不支持非master/slave的多节点部署
    val MSGID_SUNIONSTORE = 205 // 不支持非master/slave的多节点部署
    val MSGID_SMEMBERS = 220
    val MSGID_SCARD = 221
    val MSGID_SISMEMBER = 222
    val MSGID_SRANDMEMBER = 223
    val MSGID_SDIFF = 224 // 不支持非master/slave的多节点部署
    val MSGID_SINTER = 225 // 不支持非master/slave的多节点部署
    val MSGID_SUNION = 226 // 不支持非master/slave的多节点部署
    // 未实现：smove, sscan

    val MSGID_HSET = 300
    val MSGID_HSETNX = 301
    val MSGID_HMSET = 302
    val MSGID_HDEL = 303
    val MSGID_HMDEL = 304
    val MSGID_HINCRBY = 305
    val MSGID_HEXISTS = 320
    val MSGID_HLEN = 321
    val MSGID_HKEYS = 322
    val MSGID_HVALS = 323
    val MSGID_HGET = 324
    val MSGID_HGETALL = 325
    val MSGID_HMGET = 326
    // 未实现：hscan, hstrlen

    val MSGID_LPUSH = 400
    val MSGID_LPUSHM = 401
    val MSGID_LPUSHX = 402
    val MSGID_LPOP = 403
    val MSGID_LREM = 404
    val MSGID_LSET = 405
    val MSGID_LTRIM = 406
    val MSGID_LINSERT = 407
    val MSGID_RPUSH = 408
    val MSGID_RPUSHM = 409
    val MSGID_RPUSHX = 410
    val MSGID_RPOP = 411
    val MSGID_RPOPLPUSH = 412 // 不支持非master/slave的多节点部署
    val MSGID_BLPOP = 413 // 不支持非master/slave的多节点部署
    val MSGID_BRPOP = 414 // 不支持非master/slave的多节点部署
    val MSGID_BRPOPLPUSH = 415 // 不支持非master/slave的多节点部署
    val MSGID_LRANGE = 420
    val MSGID_LLEN = 421
    val MSGID_LINDEX = 422

    val MSGID_ZADD = 500
    val MSGID_ZADDMULTI = 501
    val MSGID_ZINCRBY = 502
    val MSGID_ZREM = 503
    val MSGID_ZREMMULTI = 504
    val MSGID_ZREMRANGEBYRANK = 505
    val MSGID_ZREMRANGEBYSCORE = 506
    //ZUNIONSTORE
    //ZINTERSTORE
    //ZREMRANGEBYLEX

    val MSGID_ZCARD = 520
    val MSGID_ZCOUNT = 521
    val MSGID_ZSCORE = 522    
    val MSGID_ZRANK = 523
    val MSGID_ZREVRANK = 524
    val MSGID_ZLEXCOUNT = 525
    
    val MSGID_ZRANGE = 530
    val MSGID_ZRANGEBYSCORE = 531
    val MSGID_ZREVRANGE = 532
    val MSGID_ZREVRANGEBYSCORE = 533
    val MSGID_ZRANGEBYLEX = 534

    //val MSGID_ZSCAN = 531
    
    val CRNL = "\r\n"
    val CRNL_BS = CRNL.getBytes()

    val cmdMap = HashMap[Int, String](
        MSGID_DEL -> "del",
        MSGID_EXPIRE -> "expire",
        MSGID_EXPIREAT -> "expireat",
        MSGID_EXISTS -> "exists",

        MSGID_SET -> "set",
        MSGID_GETSET -> "getset",
        MSGID_INCRBY -> "incrby", // may be incr or incrby
        MSGID_DECRBY -> "decrby", // may be decr or decrby
        MSGID_GET -> "get",
        MSGID_MGET -> "mget",

        MSGID_SADD -> "sadd",
        MSGID_SREM -> "srem",
        MSGID_SPOP -> "spop",
        MSGID_SDIFFSTORE -> "sdiffstore",
        MSGID_SINTERSTORE -> "sinterstore",
        MSGID_SUNIONSTORE -> "sunionstore",

        MSGID_SMEMBERS -> "smembers",
        MSGID_SCARD -> "scard",
        MSGID_SISMEMBER -> "sismember",
        MSGID_SRANDMEMBER -> "srandmember",
        MSGID_SDIFF -> "sdiff",
        MSGID_SINTER -> "sinter",
        MSGID_SUNION -> "sunion",

        MSGID_HSET -> "hset",
        MSGID_HSETNX -> "hsexnx",
        MSGID_HMSET -> "hmset",
        MSGID_HDEL -> "hdel",
        MSGID_HMDEL -> "hdel", // not hmdel
        MSGID_HINCRBY -> "hincrby",
        MSGID_HEXISTS -> "hexists",
        MSGID_HLEN -> "hlen",
        MSGID_HKEYS -> "hkeys",
        MSGID_HVALS -> "hvals",
        MSGID_HGET -> "hget",
        MSGID_HGETALL -> "hgetall",
        MSGID_HMGET -> "hmget",

        MSGID_LPUSH -> "lpush",
        MSGID_LPUSHM -> "lpush",
        MSGID_LPUSHX -> "lpushx",
        MSGID_LPOP -> "lpop",
        MSGID_LREM -> "lrem",
        MSGID_LSET -> "lset",
        MSGID_LTRIM -> "ltrim",
        MSGID_LINSERT -> "linsert",
        MSGID_RPUSH -> "rpush",
        MSGID_RPUSHM -> "rpush",
        MSGID_RPUSHX -> "rpushx",
        MSGID_RPOP -> "rpop",
        MSGID_RPOPLPUSH -> "rpoplpush",
        MSGID_BLPOP -> "blpop",
        MSGID_BRPOP -> "brpop",
        MSGID_BRPOPLPUSH -> "brpoplpush",
        MSGID_LRANGE -> "lrange",
        MSGID_LLEN -> "llen",
        MSGID_LINDEX -> "lindex",
     
        MSGID_ZADD -> "zadd",
        MSGID_ZADDMULTI -> "zadd",
        MSGID_ZINCRBY -> "zincrby",
        MSGID_ZREM -> "zrem",
        MSGID_ZREMMULTI -> "zrem",
        MSGID_ZREMRANGEBYRANK -> "zremrangebyrank",
        MSGID_ZREMRANGEBYSCORE -> "zremrangebyscore",
        MSGID_ZCARD -> "zcard",
        MSGID_ZCOUNT -> "zcount",
        MSGID_ZRANK -> "zrank",
        MSGID_ZREVRANK -> "zrevrank",
        MSGID_ZSCORE -> "zscore",
        MSGID_ZLEXCOUNT -> "zlexcount",
        MSGID_ZRANGE -> "zrange",
        MSGID_ZRANGEBYSCORE -> "zrangebyscore",
        MSGID_ZREVRANGE -> "zrevrange",
        MSGID_ZREVRANGEBYSCORE -> "zrevrangebyscore",
        MSGID_ZRANGEBYLEX -> "zrangebylex"
    )

    def isSetOp(msgId: Int): Boolean = {
        msgId == MSGID_SET
    }
    def isMultiKeyOp(msgId: Int): Boolean = {
        msgId == MSGID_MGET ||
        msgId == MSGID_SDIFF ||
        msgId == MSGID_SINTER ||
        msgId == MSGID_SUNION ||
        msgId == MSGID_SDIFFSTORE ||
        msgId == MSGID_SINTERSTORE ||
        msgId == MSGID_SUNIONSTORE ||
        msgId == MSGID_RPOPLPUSH ||
        msgId == MSGID_BLPOP ||
        msgId == MSGID_BRPOP ||
        msgId == MSGID_BRPOPLPUSH
    }

    def isWriteOp(msgId: Int): Boolean = {
        !isReadOp(msgId)
    }

    def isReadOp(msgId: Int): Boolean = {
        (msgId % 100) >= 20
    }

    def buildCmd(cmd: String): ChannelBuffer = {
        buildCmd(ArrayBufferString(cmd))
    }
    def buildCmd(cmd: String, key: String): ChannelBuffer = {
        buildCmd(ArrayBufferString(cmd, key))
    }
    def buildCmd(cmd: String, key: String, p3: String): ChannelBuffer = {
        buildCmd(ArrayBufferString(cmd, key, p3))
    }
    def buildCmd(cmd: String, key: String, p3: String, p4: String): ChannelBuffer = {
        buildCmd(ArrayBufferString(cmd, key, p3, p4))
    }
    def buildCmd(cmd: String, key: String, a3: ArrayBufferString): ChannelBuffer = {
        val s = ArrayBufferString(cmd, key)
        s ++= a3
        buildCmd(s)
    }
    def buildCmd(params: ArrayBufferString): ChannelBuffer = {
        val cnt = params.size
        if (params(0) == "") return null // cmd cannot be empty

        val d = ChannelBuffers.dynamicBuffer()

        var l = "*" + cnt + CRNL
        d.writeBytes(l.getBytes("utf-8"))
        for (i <- 0 until cnt) {
            val v = params(i).getBytes("utf-8")
            l = "$" + v.length + CRNL
            d.writeBytes(l.getBytes("utf-8"))
            d.writeBytes(v)
            d.writeBytes(CRNL_BS)
        }

        return d
    }

    def encode(msgId: Int, map: HashMapStringAny): Tuple2[Boolean, ChannelBuffer] = {

        if (msgId == MSGID_PING) {
            return (true, buildCmd("ping"))
        }
        if (msgId == MSGID_ASKING) {
            return (true, buildCmd("asking"))
        }
        if (msgId == MSGID_CLUSTER_SLOTS) {
            return (true, buildCmd("cluster", "slots"))
        }

        val cmd = cmdMap.getOrElse(msgId, "")
        if (cmd == "") return (false, null)

        val key = map.s("key", "")
        if (!isMultiKeyOp(msgId)) {
            if (key == "") return (false, null)
        }

        var b: ChannelBuffer = null
        msgId match {
            case MSGID_EXISTS |
                MSGID_DEL |
                MSGID_GET |
                MSGID_SMEMBERS |
                MSGID_SCARD |
                MSGID_SPOP |
                MSGID_SRANDMEMBER |
                MSGID_HLEN |
                MSGID_HKEYS |
                MSGID_HVALS |
                MSGID_HGETALL |
                MSGID_LPOP |
                MSGID_RPOP |
                MSGID_LLEN | 
                MSGID_ZCARD 
                =>
                b = buildCmd(cmd, key)
            case MSGID_EXPIRE =>
                val expire = map.i("expire")
                if (expire != 0)
                    b = buildCmd(cmd, key, expire.toString)
            case MSGID_EXPIREAT =>
                val timestamp = map.s("timestamp", "")
                if (timestamp != "")
                    b = buildCmd(cmd, key, timestamp)
            case MSGID_SET =>
                val value = map.s("value", "")
                val expire = map.i("expire")
                val nxxx = map.s("nxxx", "").toUpperCase()
                val arr = ArrayBufferString(cmd, key, value)
                if (expire > 0) {
                    arr += "EX"
                    arr += expire.toString
                }
                if (nxxx == "NX" || nxxx == "XX")
                    arr += nxxx
                b = buildCmd(arr)
            case MSGID_GETSET |
                MSGID_LPUSH |
                MSGID_LPUSHX |
                MSGID_RPUSH |
                MSGID_RPUSHX =>
                val value = map.s("value", "")
                b = buildCmd(cmd, key, value)
            case MSGID_INCRBY =>
                val value = map.s("value", "")
                if (value == "1" || value == "")
                    b = buildCmd("incr", key)
                else
                    b = buildCmd("incrby", key, value)
            case MSGID_DECRBY =>
                val value = map.s("value", "")
                if (value == "1" || value == "")
                    b = buildCmd("decr", key)
                else
                    b = buildCmd("decrby", key, value)
            case MSGID_MGET =>
                val keys = map.ls("keys")
                if (keys != null && keys.size > 0) {
                    val arr = ArrayBufferString(cmd)
                    arr ++= keys
                    b = buildCmd(arr)
                }
            case MSGID_SADD |
                MSGID_SREM |
                MSGID_LPUSHM |
                MSGID_RPUSHM =>
                val values = map.ls("values")
                if (values != null && values.size > 0)
                    b = buildCmd(cmd, key, values)
            case MSGID_SISMEMBER =>
                val value = map.s("value", "")
                if (value != "")
                    b = buildCmd(cmd, key, value)
            case MSGID_SDIFFSTORE |
                MSGID_SINTERSTORE |
                MSGID_SUNIONSTORE |
                MSGID_SDIFF |
                MSGID_SINTER |
                MSGID_SUNION =>
                val keys = map.ls("keys")
                if (keys != null && keys.size >= 2) {
                    val arr = ArrayBufferString(cmd)
                    arr ++= keys
                    b = buildCmd(arr)
                }
            case MSGID_HSET |
                MSGID_HSETNX =>
                val field = map.s("field", "")
                val value = map.s("value", "")
                if (field != "")
                    b = buildCmd(cmd, key, field, value)
            case MSGID_HMSET =>
                val keyvalues = map.lm("fieldvalues")
                if (keyvalues != null && keyvalues.size > 0) {
                    val arr = ArrayBufferString(cmd, key)
                    for (m <- keyvalues) {
                        arr += m.s("key", "")
                        arr += m.s("value", "")
                    }
                    b = buildCmd(arr)
                }
            case MSGID_HDEL |
                MSGID_HEXISTS |
                MSGID_HGET =>
                val field = map.s("field", "")
                if (field != "")
                    b = buildCmd(cmd, key, field)
            case MSGID_HMDEL |
                MSGID_HMGET =>
                val fields = map.ls("fields")
                if (fields != null && fields.size > 0) {
                    val arr = ArrayBufferString(cmd, key)
                    arr ++= fields
                    b = buildCmd(arr)
                }
            case MSGID_HINCRBY =>
                val field = map.s("field", "")
                val value = map.s("value", "")
                if (field != "" && value != "")
                    b = buildCmd(cmd, key, field, value)
            case MSGID_LREM =>
                val count = map.i("count")
                val value = map.s("value", "")
                b = buildCmd(cmd, key, count.toString, value)
            case MSGID_LSET =>
                val index = map.i("index")
                val value = map.s("value", "")
                b = buildCmd(cmd, key, index.toString, value)
            case MSGID_LTRIM |
                MSGID_LRANGE =>
                val start = map.i("start")
                val stop = map.i("stop")
                b = buildCmd(cmd, key, start.toString, stop.toString)
            case MSGID_LINSERT =>
                val beforeAfter = map.s("beforeAfter", "").toLowerCase()
                val pivot = map.s("pivot", "")
                val value = map.s("value", "")
                if (beforeAfter == "before" || beforeAfter == "after") {
                    val arr = ArrayBufferString(cmd, key)
                    arr += beforeAfter
                    arr += pivot
                    arr += value
                    b = buildCmd(arr)
                }
            case MSGID_RPOPLPUSH =>
                val source = map.s("source", "")
                val destination = map.s("destination", "")
                if (source != "" && destination != "")
                    b = buildCmd(cmd, source, destination)
            case MSGID_BLPOP |
                MSGID_BRPOP =>
                val keys = map.ls("keys")
                val timeout = map.i("timeout")
                if (keys != null && keys.size >= 1) {
                    val arr = ArrayBufferString(cmd)
                    arr ++= keys
                    arr += timeout.toString
                    b = buildCmd(arr)
                }
            case MSGID_BRPOPLPUSH =>
                val source = map.s("source", "")
                val destination = map.s("destination", "")
                val timeout = map.i("timeout")
                if (source != "" && destination != "")
                    b = buildCmd(cmd, source, destination, timeout.toString)
            case MSGID_LINDEX =>
                val index = map.i("index")
                b = buildCmd(cmd, key, index.toString)
            case MSGID_ZADD | MSGID_ZINCRBY =>
                val score = map.ns("score")
                val member = map.ns("member")
                if (score != "" && member != "")
                    b = buildCmd(cmd, key, score, member)
            case MSGID_ZADDMULTI =>
                val scoremembers = map.nlm("scoremembers")
                if ( scoremembers.size > 0) {
                    val arr = ArrayBufferString(cmd, key)
                    for (m <- scoremembers) {
                        arr += m.ns("score")
                        arr += m.ns("member")
                    }
                    b = buildCmd(arr)
                }
            case MSGID_ZREMRANGEBYRANK=>
                val start = map.ns("start")
                val stop = map.ns("stop")
                if (start != "" && stop != "")
                    b = buildCmd(cmd, key,start, stop)
            case MSGID_ZCOUNT | MSGID_ZLEXCOUNT | MSGID_ZREMRANGEBYSCORE=>
                val min = map.ns("min")
                val max = map.ns("max")
                if (min != "" && max != "")
                    b = buildCmd(cmd, key, min, max)
            case MSGID_ZSCORE | MSGID_ZRANK | MSGID_ZREVRANK | MSGID_ZREM =>
                val member = map.ns("member")
                if ( member != "")
                    b = buildCmd(cmd, key, member)
            case MSGID_ZREMMULTI =>
                val members = map.nls("members")
                if ( members.size > 0 )
                    b = buildCmd(cmd, key, members)
            case MSGID_ZRANGE | MSGID_ZREVRANGE =>
                val start = map.ns("start")
                val stop = map.ns("stop")
                val withscores = map.ns("withscores")
                if (start != "" && stop != "") {
                    val arr = ArrayBufferString(cmd, key)
                    arr += start
                    arr += stop
                    if( withscores == "1" )
                        arr += "WITHSCORES"
                    b = buildCmd(arr)
                }
            case MSGID_ZRANGEBYSCORE | MSGID_ZREVRANGEBYSCORE =>
                val min = map.ns("min")
                val max = map.ns("max")
                val withscores = map.ns("withscores")
                val offset = map.ns("offset")
                val count = map.ns("count")
                if (min != "" && max != "") {
                    val arr = ArrayBufferString(cmd, key)
                    if( msgId == MSGID_ZRANGEBYSCORE ) {
                        arr += min
                        arr += max
                    } else {
                        arr += max
                        arr += min
                    }
                    if( withscores == "1" )
                        arr += "WITHSCORES"
                    if( offset != "-1" ) {
                        arr += "LIMIT"
                        arr += offset
                        arr += count
                    }
                    b = buildCmd(arr)
                }
            case MSGID_ZRANGEBYLEX =>
                val min = map.ns("min")
                val max = map.ns("max")
                val offset = map.ns("offset")
                val count = map.ns("count")
                if (min != "" && max != "") {
                    val arr = ArrayBufferString(cmd, key)
                    arr += min
                    arr += max
                    
                    if( offset != "-1" ) {
                        arr += "LIMIT"
                        arr += offset
                        arr += count
                    }
                    b = buildCmd(arr)
                }
            case _ =>
        }

        (b != null, b)
    }

    def decode(msgId: Int, buf: ChannelBuffer, req: Request): Tuple2[Int, HashMapStringAny] = {

        val map = HashMapStringAny()
        val res = decodeInternal(buf)
        if (res == null) {
            return new Tuple2(ResultCodes.TLV_DECODE_ERROR, map)
        }
        if (!res.ok && res.value != null && res.value.startsWith("MOVED")) {
            val ss = res.value.split(" ")
            map.put("cmd", ss(0))
            map.put("slot", ss(1))
            map.put("addr", ss(2))
            return new Tuple2(MOVED_ERROR, map)
        }
        if (!res.ok && res.value != null && res.value.startsWith("ASK")) {
            val ss = res.value.split(" ")
            map.put("cmd", ss(0))
            map.put("slot", ss(1))
            map.put("addr", ss(2))
            return new Tuple2(ASK_ERROR, map)
        }

        var code = if (res.ok) 0 else ResultCodes.CACHE_UPDATEFAILED

        msgId match {
            case MSGID_PING |
                MSGID_SET |
                MSGID_HMSET |
                MSGID_LSET |
                MSGID_LTRIM =>
            case MSGID_EXISTS |
                MSGID_DEL |
                MSGID_EXPIRE |
                MSGID_EXPIREAT |
                MSGID_SADD |
                MSGID_SREM |
                MSGID_SCARD |
                MSGID_SISMEMBER |
                MSGID_SDIFFSTORE |
                MSGID_SINTERSTORE |
                MSGID_SUNIONSTORE |
                MSGID_HSET |
                MSGID_HSETNX |
                MSGID_HDEL |
                MSGID_HMDEL |
                MSGID_HEXISTS |
                MSGID_HLEN |
                MSGID_LPUSH |
                MSGID_LPUSHM |
                MSGID_LPUSHX |
                MSGID_LREM |
                MSGID_LINSERT |
                MSGID_RPUSH |
                MSGID_RPUSHM |
                MSGID_RPUSHX |
                MSGID_LLEN |
                MSGID_ZADD |
                MSGID_ZADDMULTI |
                MSGID_ZCARD |
                MSGID_ZCOUNT |
                MSGID_ZLEXCOUNT |
                MSGID_ZREM |
                MSGID_ZREMMULTI |
                MSGID_ZREMRANGEBYRANK |
                MSGID_ZREMRANGEBYSCORE 
                =>
                if (res.ok) {
                    try {
                        map.put("count", res.value.toInt)
                    } catch { case e: Throwable => }
                }
            case MSGID_GET |
                MSGID_GETSET |
                MSGID_SPOP |
                MSGID_SRANDMEMBER |
                MSGID_HGET |
                MSGID_LPOP |
                MSGID_RPOP |
                MSGID_RPOPLPUSH |
                MSGID_BRPOPLPUSH |
                MSGID_LINDEX =>
                if (res.ok && res.value != null)
                    map.put("value", res.value)
                else if (res.ok && res.value == null)
                    code = ResultCodes.CACHE_NOT_FOUND
            case MSGID_ZINCRBY | MSGID_ZSCORE  =>
                if (res.ok && res.value != null)
                    map.put("score", res.value)
                else if (res.ok && res.value == null)
                    code = ResultCodes.CACHE_NOT_FOUND
            case MSGID_INCRBY |
                MSGID_DECRBY |
                MSGID_HINCRBY =>
                if (res.ok) {
                    try {
                        map.put("value", res.value.toInt)
                    } catch { case e: Throwable => }
                }
            case MSGID_ZRANK |
                MSGID_ZREVRANK  =>
                if (res.ok) {
                    try {
                        map.put("rank", res.value.toInt)
                    } catch { case e: Throwable => }
                }
            case MSGID_MGET |
                MSGID_HMGET =>
                val fieldKeys = if (msgId == MSGID_MGET) "keys" else "fields"
                val fieldValues = if (msgId == MSGID_MGET) "keyvalues" else "fieldvalues"
                if (res.ok) {
                    val arr = res.arr
                    if (arr != null && arr.size > 0) {
                        val values = ArrayBufferString()
                        for (a <- arr) {
                            values += a.value
                        }
                        val keys = req.ls(fieldKeys)
                        var i = 0
                        val buf = ArrayBufferMap()
                        while (i < keys.size) {
                            buf += HashMapStringAny("key" -> keys(i), "value" -> values(i))
                            i += 1
                        }
                        map.put(fieldValues, buf)
                    }
                }
            case MSGID_SMEMBERS |
                MSGID_SDIFF |
                MSGID_SINTER |
                MSGID_SUNION |
                MSGID_HKEYS |
                MSGID_HVALS |
                MSGID_LRANGE =>
                val fieldValues = if (msgId == MSGID_HKEYS) "fields" else "values"
                if (res.ok) {
                    val arr = res.arr
                    if (arr != null && arr.size > 0) {
                        val values = ArrayBufferString()
                        for (a <- arr) {
                            values += a.value
                        }
                        map.put(fieldValues, values)
                    }
                }
            case MSGID_HGETALL =>
                if (res.ok) {
                    val arr = res.arr
                    if (arr != null && arr.size > 0) {
                        var i = 0
                        val buf = ArrayBufferMap()
                        while (i < arr.size) {
                            buf += HashMapStringAny("key" -> arr(i).value, "value" -> arr(i + 1).value)
                            i += 2
                        }
                        map.put("fieldvalues", buf)
                    }
                }
            case MSGID_BLPOP |
                MSGID_BRPOP =>
                if (res.ok) {
                    val arr = res.arr
                    if (arr != null && arr.size == 2) {
                        map.put("key", arr(0).value)
                        map.put("value", arr(1).value)
                    }
                }
            case MSGID_CLUSTER_SLOTS =>
                if (res.ok) {
                    val arr = res.arr
                    if (arr != null && arr.size > 0) {

                        map.put("slots", arr.size)
                        for (i <- 0 until arr.size) {
                            val slot_arr = arr(i).arr
                            val min = slot_arr(0).value.toInt
                            val max = slot_arr(1).value.toInt
                            val ip = slot_arr(2).arr(0).value
                            val port = slot_arr(2).arr(1).value
                            map.put("slot-" + i + "-min", min)
                            map.put("slot-" + i + "-max", max)
                            map.put("slot-" + i + "-addr", ip + ":" + port)
                        }
                    } else {
                        map.put("slots", 0)
                    }
                }
            case MSGID_ZRANGE | 
                MSGID_ZREVRANGE | 
                MSGID_ZRANGEBYSCORE | 
                MSGID_ZREVRANGEBYSCORE | 
                MSGID_ZRANGEBYLEX =>
                if (res.ok) {
                    val arr = res.arr
                    if (arr != null && arr.size > 0) {
                        val withscores = req.ns("withscores")
                        if( withscores == "1") {
                            var i = 0
                            val buf = ArrayBufferMap()
                            while (i < arr.size) {
                                buf += HashMapStringAny("member" -> arr(i).value, "score" -> arr(i + 1).value)
                                i += 2
                            }
                            map.put("scoremembers", buf)
                        } else {
                            var i = 0
                            val buf = ArrayBufferString()
                            while (i < arr.size) {
                                buf += arr(i).value
                                i += 1
                            }
                            map.put("members", buf)
                        }
                    }
                }
                
            case _ =>
                code = ResultCodes.TLV_DECODE_ERROR
        }
        (code, map)
    }

    def decodeInternal(buf: ChannelBuffer): RedisResponse = {
        val len = buf.readableBytes()
        if (len < 4) {
            return null;
        }

        var s = buf.readerIndex
        val max = s + len - 1

        val (valid, res) = decodeRecur(buf, s, max)
        res
    }

    def decodeRecur(buf: ChannelBuffer, s: Int, max: Int): Tuple2[Int, RedisResponse] = {

        val len = max - s + 1
        val ch = buf.getByte(s).toChar
        var e = findCrNl(buf, s, max)
        if (e < 0) return new Tuple2(-1, null)

        var valid = e - s + 1
        ch match {
            case '+' | ':' =>
                val str = parseStr(buf, s + 1, e - 2)
                return new Tuple2(valid, new RedisResponse(true, str))
            case '-' =>
                val str = parseStr(buf, s + 1, e - 2)
                return new Tuple2(valid, new RedisResponse(false, str))
            case '$' =>
                val num = parseNumber(buf, s, e)
                if (num < -1)
                    return new Tuple2(-1, null)

                if (num >= 0) {
                    if (len < valid + num + 2) return new Tuple2(-1, null)
                    val cr = buf.getByte(s + valid + num).toChar
                    val nl = buf.getByte(s + valid + num + 1).toChar
                    if (cr != '\r' || nl != '\n')
                        return new Tuple2(-1, null)
                    val str = parseStr(buf, s + valid, s + valid + num - 1)
                    valid += num + 2
                    return new Tuple2(valid, new RedisResponse(true, str))
                } else { // -1
                    return new Tuple2(valid, new RedisResponse(true, null))
                }

            case '*' =>
                val params = parseNumber(buf, s, e)
                if (params < -1)
                    return new Tuple2(-1, null)
                if (params == -1)
                    return new Tuple2(valid, new RedisResponse(true, null, null))
                val arr = new ArrayBuffer[RedisResponse]()
                var i = 0
                var ts = e + 1
                while (i < params) {

                    val (v, res2) = decodeRecur(buf, ts, max)
                    if (v < 0 || res2 == null) return new Tuple2(-1, null)
                    arr += res2

                    valid += v
                    ts += v

                    i += 1
                }

                return new Tuple2(valid, new RedisResponse(true, null, arr))
            case _ =>
                return new Tuple2(-1, null)
        }

        return new Tuple2(-1, null)
    }

    // find last position of ...\r\n
    def findCrNl(buf: ChannelBuffer, min: Int, max: Int): Int = {
        var i = min
        while (i <= max) {
            val ch = buf.getByte(i)
            if (ch == '\n') {
                val lastbyte = buf.getByte(i - 1)
                if (lastbyte == '\r') {
                    return i
                }
            }
            i += 1
        }
        -1
    }

    def parseStr(buf: ChannelBuffer, min: Int, max: Int): String = {
        val len = max - min + 1
        val bs = new Array[Byte](len)
        buf.getBytes(min, bs)
        new String(bs, "utf-8")
    }

    def parseNumber(buf: ChannelBuffer, min: Int, max: Int): Int = {
        var i = min
        var s = ""
        while (i <= max) {
            val ch = buf.getByte(i).toChar
            if (ch == '-' || ch >= '0' && ch <= '9') s += ch
            i += 1
        }
        try {
            s.toInt
        } catch {
            case e: Throwable =>
                Int.MinValue
        }
    }

}

class RedisFrameDecoder extends FrameDecoder with Logging {

    override def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): Object = {

        try {
            val len = buf.readableBytes()
            if (len < 4) {
                return null;
            }

            var s = buf.readerIndex
            val max = s + len - 1

            val valid = getValid(buf, s, max)
            if (valid < 0) {
                return null
            }

            val frame = buf.readBytes(valid);
            return frame
        } catch {
            case e: Throwable =>
                log.error("redis frame decode exception e=" + e.getMessage, e)
                throw e
        }
    }

    /*
    Null Bulk String:  $-1\r\n
    空数组:  *0\r\n
    空Array: *-1\r\n

    嵌套
     *2\r\n
     *3\r\n
     :1\r\n
     :2\r\n
     :3\r\n
     *2\r\n
     +Foo\r\n
     -Bar\r\n
     */

    def getValid(buf: ChannelBuffer, s: Int, max: Int): Int = {

        val len = max - s + 1
        val ch = buf.getByte(s).toChar
        var e = findCrNl(buf, s, max)
        if (e < 0) return -1

        var valid = e - s + 1
        ch match {
            case '+' | '-' | ':' =>
                return valid
            case '$' =>
                val num = parseNumber(buf, s, e)
                if (num < -1) // can be -1
                    throw new Exception("number is not valid")

                if (num >= 0) {
                    if (len < valid + num + 2) return -1
                    val cr = buf.getByte(s + valid + num).toChar
                    val nl = buf.getByte(s + valid + num + 1).toChar
                    if (cr != '\r' || nl != '\n')
                        throw new Exception("not a valid cr nl")
                    valid += num + 2
                    return valid
                } else if (num == -1) {
                    return valid
                }
                return valid
            case '*' =>
                val params = parseNumber(buf, s, e)
                if (params < -1) // can be -1
                    throw new Exception("number is not valid")

                var i = 0
                var ts = e + 1
                while (i < params) {
                    if (ts > max) return -1
                    val v = getValid(buf, ts, max)
                    if (v < 0) return -1
                    valid += v
                    ts += v

                    i += 1
                }
                return valid
            case _ =>
                throw new Exception("redis frame is not correct")
        }

        return -1
    }

    def findCrNl(buf: ChannelBuffer, min: Int, max: Int): Int = { // find last position of ...\r\n
        var i = min
        while (i <= max) {
            val ch = buf.getByte(i)
            if (ch == '\n') {
                val lastbyte = buf.getByte(i - 1)
                if (lastbyte == '\r') {
                    return i
                }
            }
            i += 1
        }
        -1
    }

    def parseNumber(buf: ChannelBuffer, min: Int, max: Int): Int = { // read ($|*){number}\r\n skip chars not in [0-9-]
        var i = min
        var s = ""
        while (i <= max) {
            val ch = buf.getByte(i).toChar
            if (ch == '-' || ch >= '0' && ch <= '9') s += ch
            i += 1
        }

        try {
            s.toInt
        } catch {
            case e: Throwable =>
                throw new Exception("number is not correct, s=" + s)
        }
    }

}


