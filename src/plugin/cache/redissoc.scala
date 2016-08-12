package jvmdbbroker.plugin.cache

import java.util.{TreeMap,Timer,TimerTask}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger,AtomicBoolean}
import org.jboss.netty.buffer._;

import scala.collection.mutable.{ArrayBuffer,HashMap}

import RedisSoc._
import jvmdbbroker.core._

object RedisSoc {
    val TYPE_UNKNOWN = -1
    val TYPE_ARRAYHASH = 1
    val TYPE_CONHASH = 2
    val TYPE_MASTERSLAVE = 3
}

class RedisSoc(
    val addrs:String,
    val cacheType:Int,
    val receiver_f: (Any)=>Unit,
    val connectTimeout :Int = 15000,
    val pingInterval: Int = 60000,
    val connSizePerAddr: Int = 4,
    val timerInterval :Int = 100,
    val reconnectInterval : Int = 1,
    val failOver:Boolean = true,
    val maxErrorCount:Int = 50
) extends Logging with Dumpable {

    val addrArray = addrs.split(",")
    val addrSize = addrArray.size

    val addrValid = new Array[AtomicBoolean](addrSize)
    val addrErrCnt = new Array[AtomicInteger](addrSize)
    var timer:Timer = _

    var treeMap : TreeMap[Long,Int] = _

    var nettyClient : RedisNettyClient = _
    val generator = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[Int,CacheData]()

    init

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = nettyClient.selfcheck()
        buff
    }

    def dump() {

        val buff = new StringBuilder
        buff.append("dataMap.size=").append(dataMap.size).append(",")
        log.info(buff.toString)

        nettyClient.dump
    }

    def init() {

        for(i <- 0 until addrSize ) addrValid(i) = new AtomicBoolean(true)
        for(i <- 0 until addrSize ) addrErrCnt(i) = new AtomicInteger(0)
        if( cacheType == TYPE_CONHASH ) initTreeMap()

        timer = new Timer("redissoctimer")

        timer.schedule( new TimerTask() {
            def run() {
                testOfflineAddrs()
            }
        }, 5000, 1000 )

        nettyClient = new RedisNettyClient(this,
            addrs,
            connectTimeout,
            pingInterval,
            connSizePerAddr,
            timerInterval,
            reconnectInterval
            )
        log.info("redis soc {} started",addrs)
    }

    def initTreeMap() {
        treeMap = new TreeMap[Long,Int]()
        for( i <- 0 until addrSize ) {
            for( j <- 0 until 50 ) {
                val h = MurmurHash.hash(addrArray(i)+"-"+i+"-"+j)  
                treeMap.put(h,i)
            }
        }
    }

    def close() {
        timer.cancel()
        nettyClient.close()
        log.info("redis soc {} stopped",addrs)
    }

    def generateSequence():Int = {
        generator.getAndIncrement()
    }

    def selectAddrIdx(key:String,times:Int):Int = {
        cacheType match {
            case TYPE_ARRAYHASH =>
                val h = MurmurHash.hash(key)
                val d = h % addrSize
                var idx = if( d < 0 ) (-1 * d).toInt else d.toInt
                if( failOver ) nextValid(idx)
                else idx
            case TYPE_CONHASH =>
                val h = MurmurHash.hash(key)  
                val sortedMap = treeMap.tailMap(h)
                var idx = 0
                if( sortedMap == null || sortedMap.isEmpty )
                    idx = treeMap.get( treeMap.firstKey ).toInt
                else
                    idx = treeMap.get( sortedMap.firstKey ).toInt
                if( failOver ) nextValid(idx)
                else idx
            case TYPE_MASTERSLAVE =>
                var idx = if( times == 1 ) 0 else 1
                if( failOver && idx == 0 ) nextValid(idx) 
                else idx
        }
    }

    def nextValid(idx:Int):Int = {
        var i = idx 
        if( addrValid(i).get() ) return i
        i += 1
        if( i >= addrSize ) i = 0
        while( !addrValid(i).get() && i != idx ) {
            i += 1
            if( i >= addrSize ) i = 0
        }
        i
    }

    def send(req: Request, timeout: Int, toAddr:Int = -1, times:Int = 1):Unit = {

        val keys = req.ls("keys")
        if( req.msgId != RedisCodec.MSGID_MGET || keys == null || keys.size == 0 || cacheType == TYPE_MASTERSLAVE || addrSize == 1) {
            sendToSingleAddr(req,timeout,toAddr,times)
            return
        }

        // 特殊逻辑：处理mget时keys分布在不同的redis实例情况

        val m = new HashMap[Int,ArrayBufferString]()
        for( key <- keys ) {
            val addrIdx = selectAddrIdx(key,1) 
            var arr = m.getOrElse(addrIdx,null)
            if( arr == null ) { 
                arr = new ArrayBufferString()
                m.put(addrIdx,arr)
            }
            arr += key
        }
        if( m.size == 1 ) {
            val to = m.keys.toList.head
            sendToSingleAddr(req,timeout,to,1)
            return
        }

        var i=0

        // save the parent request
        val sequence = generateSequence()
        val data = new CacheData(req,timeout,times,-1)
        data.responses = new Array[Response](m.size)
        dataMap.put(sequence,data)

        for( (to,keys) <- m ) {
            val subreqid = "sub:"+req.requestId + ":" + i
            val body = HashMapStringAny("keys"->keys,"parentSequence"->sequence)
            val subreq = new Request(
                subreqid,"dummy",0,0,req.serviceId,req.msgId,null,
            body,null
        )
            sendToSingleAddr(subreq,timeout,to,times)
            i += 1
        }

    }

    def sendToSingleAddr(req: Request, timeout: Int, toAddr:Int = -1, times:Int = 1):Unit = {

        val (ok1,buf) = RedisCodec.encode(req.msgId,req.body)
        if( !ok1 ) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR,req)
            receiver_f(new RequestResponseInfo(req,res))
            return
        }

        val sequence = generateSequence()

        var key = req.s("key","")
        if( key == "") {
            var keys = req.ls("keys")
            if( keys != null && keys.size > 0 ) key = keys(0)
        }
        if( key == "") {
            var source = req.s("source","")
            if( source != "" ) key = source
        }
        val addrIdx = if( toAddr == -1 ) selectAddrIdx(key,times) else toAddr
        //println("cacheType=%d,key=%s,addr=%d".format(cacheType,key,addrIdx))
        dataMap.put(sequence,new CacheData(req,timeout,times,addrIdx))

        var ok2 = nettyClient.sendByAddr(sequence,buf,timeout,addrIdx)
        if( !ok2 ) {
            val connId = addrArray(addrIdx)+":0"
            processError(sequence,connId,ResultCodes.SOC_NOCONNECTION)
        }
    }

    def testOfflineAddrs() {

        val timeout = 3000
        var buf:ChannelBuffer = null

        for( i <- 0 until addrSize if !addrValid(i).get() ) {
            if( buf == null ) {
                val (ok,buf2) = RedisCodec.encode(RedisCodec.MSGID_PING,HashMapStringAny())
                buf = buf2
            }
            val sequence = generateSequence()
            dataMap.put(sequence,new CacheData(null,timeout,1,i,true)) // req is null
            var ok2 = nettyClient.sendByAddr(sequence,buf,timeout,i)
            if( !ok2 ) {
                val connId = addrArray(i)+":0"
                processError(sequence,connId,ResultCodes.SOC_NOCONNECTION)
            }
        }

    }

    def receive(sequence:Int,buf:ChannelBuffer, connId:String):Unit = {
        val saved = dataMap.remove(sequence)
        if( saved == null ) {
            return
        } 

        addrOk(saved.addrIdx)
        if( saved.testOnly ) return

        val req = saved.data
        if( saved.times == 1 ) { // for hash or master
            val (errorCode,body) = RedisCodec.decode(req.msgId,buf,req)
            val res = new Response(errorCode,body,req)
            res.remoteAddr = parseRemoteAddr(connId)
            if( req.requestId.startsWith("sub:")) { // mget 子请求
                processMgetResponse(req,res)
                return
            }
            receiver_f(new RequestResponseInfo(req,res))
            postReceive(req,saved.addrIdx)

            if( cacheType == TYPE_MASTERSLAVE  && RedisCodec.isWriteOp(req.msgId) ) {
                val addr = if( saved.addrIdx == 0 ) 1 else 0
                send(req,saved.timeout,addr,2) // write to slave(depends the addr first read), ignore the response(times=2)
            }
        }
        if( saved.times == 2 ) { // only for slave
            postReceive(req,saved.addrIdx)
        }
    }

    def postReceive(req:Request,addrIdx:Int) {
        val msgId = req.msgId
        if( RedisCodec.isWriteOp(msgId) && !RedisCodec.isSetOp(msgId) ) {
            val expire = req.i("expire")
            if( expire > 0 ) {
                val (ok,buf) = RedisCodec.encode(RedisCodec.MSGID_EXPIRE,req.body)
                val timeout = 3000
                val sequence = generateSequence()
                nettyClient.sendByAddr(sequence,buf,timeout,addrIdx)
            }
        }
    }

    def networkError(sequence:Int,connId:String) {
        processError(sequence,connId,ResultCodes.SOC_NOCONNECTION)
    }
    def timeoutError(sequence:Int,connId:String) {
        processError(sequence,connId,ResultCodes.SOC_TIMEOUT)
    }

    def processError(sequence:Int,connId:String,errorCode:Int) {
        val saved = dataMap.remove(sequence)
        if( saved == null ) {
            return
        } 

        if( errorCode != ResultCodes.SOC_TIMEOUT)
            addrFailed(saved.addrIdx)

        if( saved.testOnly ) return

        val req = saved.data
        if( saved.times == 1 ) { // for hash or master

            if( cacheType == TYPE_MASTERSLAVE  && RedisCodec.isReadOp(req.msgId) && saved.addrIdx == 0) {
                send(req,saved.timeout,1,1) // read from slave(toAddr=1), need the response (times=1)
                return
            }

            val res = createErrorResponse(errorCode,req)
            res.remoteAddr = parseRemoteAddr(connId)
            if( req.requestId.startsWith("sub:")) { // mget 子请求
                processMgetResponse(req,res)
                return
            }
            receiver_f(new RequestResponseInfo(req,res))
        }

    }

    def processMgetResponse(subreq:Request,subRes:Response) {
        val p = subreq.requestId.lastIndexOf(":")
        if( p < 0 ) return
        val idx = subreq.requestId.substring(p+1).toInt
        val parentSequence = subreq.i("parentSequence")
        val parentData = dataMap.get(parentSequence)
        if( parentData == null ) return
        if( idx < 0 || idx >= parentData.responses.size ) return
        parentData.responses(idx) = subRes
        val finished = parentData.responses.forall(_ != null)
        if( !finished ) return
        dataMap.remove(parentSequence)
        val errorResponse = parentData.responses.filter(_.code < 0)
        val req = parentData.data
        if( errorResponse.size > 0 ) {
            val first = errorResponse(0)
            val res = new Response(first.code,HashMapStringAny(),req)
            res.remoteAddr = first.remoteAddr
            receiver_f(new RequestResponseInfo(req,res))
            return
        }
        val keyvalues = ArrayBufferMap()
        for( res <- parentData.responses ) 
            keyvalues ++= res.body.lm("keyvalues")

        val res = new Response(0,HashMapStringAny("keyvalues"->keyvalues),req)
        res.remoteAddr = parentData.responses(0).remoteAddr
        receiver_f(new RequestResponseInfo(req,res))
    }

    def addrOk(addrIdx:Int) {
        if( !failOver ) return
        var cnt = addrErrCnt(addrIdx).intValue()
        if( cnt >= 1 ) cnt = addrErrCnt(addrIdx).decrementAndGet()
        if( cnt <= 0  &&  !addrValid(addrIdx).get() ) {
            addrValid(addrIdx).set(true)
            log.warn("set addr to online, addr="+addrArray(addrIdx))
        }
    }

    def addrFailed(addrIdx:Int) {
        if( !failOver ) return
        var cnt = addrErrCnt(addrIdx).intValue()
        if( cnt < maxErrorCount ) cnt = addrErrCnt(addrIdx).incrementAndGet()
        if( cnt >= maxErrorCount &&  addrValid(addrIdx).get() ) {
            addrValid(addrIdx).set(false)
            log.warn("set addr to offline, addr="+addrArray(addrIdx))
        }
    }

    def generatePing():Tuple2[Int,ChannelBuffer] = {
        val sequence = generateSequence()
        val (ok,buf) = RedisCodec.encode(RedisCodec.MSGID_PING,HashMapStringAny())
        (sequence,buf)
    }

    def createErrorResponse(code:Int,req:Request):Response = {
        val res = new Response (code,new HashMapStringAny(),req)
        res
    }

    def parseRemoteAddr(connId:String):String = {
        val p = connId.lastIndexOf(":")

        if (p >= 0)
            connId.substring(0,p)
        else
            "0.0.0.0:0"
    }


    class CacheData(val data:Request, val timeout:Int, val times:Int, val addrIdx:Int,val testOnly:Boolean = false) {
        var responses:Array[Response] = null
    }

    class RedisResponse(val ok:Boolean,val value:String,val arr:ArrayBuffer[RedisResponse] = null)

    object RedisCodec {

        val MSGID_PING = 1
        val MSGID_DEL = 2
        val MSGID_EXPIRE = 3
        val MSGID_EXPIREAT = 4
        val MSGID_EXISTS = 20

        val MSGID_SET = 100
        val MSGID_GETSET = 101
        val MSGID_INCRBY = 102
        val MSGID_DECRBY = 103
        val MSGID_GET = 120
        val MSGID_MGET = 121 // 支持多节点部署

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

        val CRNL = "\r\n"
        val CRNL_BS = CRNL.getBytes()

        val cmdMap = HashMap[Int,String] (
            MSGID_PING -> "ping",
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
            MSGID_LINDEX -> "lindex"
        )

        def isSetOp(msgId:Int):Boolean = {
            msgId == MSGID_SET
        }
        def isMultiKeyOp(msgId:Int):Boolean = {
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

        def isWriteOp(msgId:Int):Boolean = {
            !isReadOp(msgId)
        }

        def isReadOp(msgId:Int):Boolean = {
            ( msgId % 100 ) >= 20
        }

        def buildCmd(cmd:String):ChannelBuffer = {
            buildCmd(ArrayBufferString(cmd))
        }
        def buildCmd(cmd:String,key:String):ChannelBuffer = {
            buildCmd(ArrayBufferString(cmd,key))
        }
        def buildCmd(cmd:String,key:String,p3:String):ChannelBuffer = {
            buildCmd(ArrayBufferString(cmd,key,p3))
        }
        def buildCmd(cmd:String,key:String,p3:String,p4:String):ChannelBuffer = {
            buildCmd(ArrayBufferString(cmd,key,p3,p4))
        }
        def buildCmd(cmd:String,key:String,a3:ArrayBufferString):ChannelBuffer = {
            val s = ArrayBufferString(cmd,key)
            s ++= a3
            buildCmd(s)
        }
        def buildCmd(params:ArrayBufferString):ChannelBuffer = {
            val cnt = params.size
            if( params(0) == "" ) return null // cmd cannot be empty

            val d = ChannelBuffers.dynamicBuffer()

            var l = "*"+cnt+CRNL
            d.writeBytes(l.getBytes("utf-8"))
            for(i <- 0 until cnt ) {
                val v = params(i).getBytes("utf-8")
                l = "$"+v.length+CRNL
                d.writeBytes(l.getBytes("utf-8"))
                d.writeBytes(v)
                d.writeBytes(CRNL_BS)
            }

            return d
        }

        def encode(msgId:Int,map:HashMapStringAny):Tuple2[Boolean,ChannelBuffer] = {

            val cmd = cmdMap.getOrElse(msgId,"")
            if( cmd == "" ) return (false,null)

            val key = map.s("key","")
            if( msgId != MSGID_PING && !isMultiKeyOp(msgId) ) {
                if( key == "" ) return (false,null)
            }

            var b : ChannelBuffer = null
            msgId match {
                case MSGID_PING =>
                    b = buildCmd(cmd)
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
                    MSGID_LLEN =>
                    b = buildCmd(cmd,key)
                case MSGID_EXPIRE =>
                    val expire = map.i("expire")
                    if( expire != 0 )
                        b = buildCmd(cmd,key,expire.toString)
                case MSGID_EXPIREAT =>
                    val timestamp = map.s("timestamp","")
                    if( timestamp != "" )
                        b = buildCmd(cmd,key,timestamp)
                case MSGID_SET =>
                    val value = map.s("value","")
                    val expire = map.i("expire")
                    val nxxx = map.s("nxxx","").toUpperCase()
                    val arr = ArrayBufferString(cmd,key,value)
                    if( expire > 0 ) {
                        arr += "EX"
                        arr += expire.toString
                    }
                    if( nxxx == "NX" || nxxx == "XX")
                        arr += nxxx
                    b = buildCmd(arr)
                case MSGID_GETSET |
                    MSGID_LPUSH |
                    MSGID_LPUSHX |
                    MSGID_RPUSH |
                    MSGID_RPUSHX =>
                    val value = map.s("value","")
                    b = buildCmd(cmd,key,value)
                case MSGID_INCRBY =>
                    val value = map.s("value","")
                    if( value == "1" || value == "")
                        b = buildCmd("incr",key)
                    else 
                        b = buildCmd("incrby",key,value)
                case MSGID_DECRBY =>
                    val value = map.s("value","")
                    if( value == "1" || value == "")
                        b = buildCmd("decr",key)
                    else
                        b = buildCmd("decrby",key,value)
                case MSGID_MGET =>
                    val keys = map.ls("keys")
                    if( keys != null && keys.size > 0) {
                        val arr = ArrayBufferString(cmd)
                        arr ++= keys
                        b = buildCmd(arr)
                    }
                case MSGID_SADD | 
                    MSGID_SREM |
                    MSGID_LPUSHM |
                    MSGID_RPUSHM =>
                    val values = map.ls("values")
                    if( values != null && values.size > 0)
                        b = buildCmd(cmd,key,values)
                case MSGID_SISMEMBER =>
                    val value = map.s("value","")
                    if( value != "" )
                        b = buildCmd(cmd,key,value)
                case MSGID_SDIFFSTORE |
                    MSGID_SINTERSTORE |
                    MSGID_SUNIONSTORE |
                    MSGID_SDIFF |
                    MSGID_SINTER |
                    MSGID_SUNION =>
                val keys = map.ls("keys")
                if( keys != null && keys.size >= 2) {
                    val arr = ArrayBufferString(cmd)
                    arr ++= keys
                    b = buildCmd(arr)
                }
                case MSGID_HSET | 
                    MSGID_HSETNX =>
                    val field = map.s("field","")
                    val value = map.s("value","")
                    if( field != "")
                        b = buildCmd(cmd,key,field,value)
                case MSGID_HMSET =>
                    val keyvalues = map.lm("fieldvalues")
                    if( keyvalues != null && keyvalues.size > 0 ) {
                        val arr = ArrayBufferString(cmd,key)
                        for( m <- keyvalues ) {
                            arr += m.s("key","")
                            arr += m.s("value","")
                        }
                        b = buildCmd(arr)
                    }
                case MSGID_HDEL | 
                    MSGID_HEXISTS | 
                    MSGID_HGET =>
                    val field = map.s("field","")
                    if( field != "")
                        b = buildCmd(cmd,key,field)
                case MSGID_HMDEL |
                    MSGID_HMGET =>
                    val fields = map.ls("fields")
                    if( fields != null && fields.size > 0 ) {
                        val arr = ArrayBufferString(cmd,key)
                        arr ++= fields
                        b = buildCmd(arr)
                    }
                case MSGID_HINCRBY =>
                    val field = map.s("field","")
                    val value = map.s("value","")
                    if( field != "" && value != "" )
                        b = buildCmd(cmd,key,field,value)
                case MSGID_LREM =>
                    val count = map.i("count")
                    val value = map.s("value","")
                    b = buildCmd(cmd,key,count.toString,value)
                case MSGID_LSET =>
                    val index = map.i("index")
                    val value = map.s("value","")
                    b = buildCmd(cmd,key,index.toString,value)
                case MSGID_LTRIM |
                    MSGID_LRANGE =>
                    val start = map.i("start")
                    val stop = map.i("stop")
                    b = buildCmd(cmd,key,start.toString,stop.toString)
                case MSGID_LINSERT =>
                    val beforeAfter = map.s("beforeAfter","").toLowerCase()
                    val pivot = map.s("pivot","")
                    val value = map.s("value","")
                    if( beforeAfter == "before" || beforeAfter == "after" ) {
                        val arr = ArrayBufferString(cmd,key)
                        arr += beforeAfter
                        arr += pivot
                        arr += value
                        b = buildCmd(arr)
                    }
                case MSGID_RPOPLPUSH =>
                    val source = map.s("source","")
                    val destination = map.s("destination","")
                    if( source != "" && destination != "")
                        b = buildCmd(cmd,source,destination)
                case MSGID_BLPOP |
                    MSGID_BRPOP =>
                    val keys = map.ls("keys")
                    val timeout = map.i("timeout")
                    if( keys != null && keys.size >= 1) {
                        val arr = ArrayBufferString(cmd)
                        arr ++= keys
                        arr += timeout.toString
                        b = buildCmd(arr)
                    }
                case MSGID_BRPOPLPUSH =>
                    val source = map.s("source","")
                    val destination = map.s("destination","")
                    val timeout = map.i("timeout")
                    if( source != "" && destination != "")
                        b = buildCmd(cmd,source,destination,timeout.toString)
                case MSGID_LINDEX =>
                    val index = map.i("index")
                    b = buildCmd(cmd,key,index.toString)
                case _ =>
            }

            (b != null,b)
        }

        def decode(msgId:Int,buf:ChannelBuffer,req:Request):Tuple2[Int,HashMapStringAny] = {

            val map = HashMapStringAny()
            val res = decodeInternal(buf)
            if( res == null ) {
                return new Tuple2(ResultCodes.TLV_DECODE_ERROR,map)
            }

            var code = if( res.ok ) 0 else ResultCodes.CACHE_UPDATEFAILED

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
                    MSGID_LLEN =>
                    if( res.ok ) {
                        try {
                            map.put("count",res.value.toInt)
                        } catch { case e:Throwable => }
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
                    if( res.ok && res.value != null ) 
                        map.put("value",res.value)
                    else if( res.ok && res.value == null ) 
                        code = ResultCodes.CACHE_NOT_FOUND
                case MSGID_INCRBY | 
                    MSGID_DECRBY | 
                    MSGID_HINCRBY =>
                if( res.ok ){
                    try {
                        map.put("value",res.value.toInt)
                    } catch { case e:Throwable => }
                }
                case MSGID_MGET | 
                    MSGID_HMGET =>
                    val fieldKeys = if( msgId == MSGID_MGET ) "keys" else "fields"
                    val fieldValues = if( msgId == MSGID_MGET ) "keyvalues" else "fieldvalues"
                    if( res.ok ) {
                        val arr = res.arr
                        if( arr != null && arr.size > 0 ) {
                            val values = ArrayBufferString()
                            for(a <- arr) {
                                values += a.value
                            }
                            val keys = req.ls(fieldKeys)
                            var i = 0 
                            val buf = ArrayBufferMap()
                            while( i < keys.size ) {
                                buf += HashMapStringAny( "key" -> keys(i), "value" -> values(i) )
                                i += 1
                            }
                            map.put(fieldValues,buf)
                        }
                    }
                case MSGID_SMEMBERS | 
                    MSGID_SDIFF |
                    MSGID_SINTER |
                    MSGID_SUNION |
                    MSGID_HKEYS |
                    MSGID_HVALS | 
                    MSGID_LRANGE => 
                    val fieldValues = if( msgId == MSGID_HKEYS ) "fields" else "values"
                    if( res.ok ) {
                        val arr = res.arr
                        if( arr != null && arr.size > 0 ) {
                            val values = ArrayBufferString()
                            for(a <- arr) {
                                values += a.value
                            }
                            map.put(fieldValues,values)
                        }
                    }
                case MSGID_HGETALL =>
                    if( res.ok ) {
                        val arr = res.arr
                        if( arr != null && arr.size > 0 ) {
                            var i = 0 
                            val buf = ArrayBufferMap()
                            while( i < arr.size ) {
                                buf += HashMapStringAny( "key" -> arr(i).value, "value" -> arr(i+1).value )
                                i += 2
                            }
                            map.put("fieldvalues",buf)
                        }
                    }
                case MSGID_BLPOP |
                    MSGID_BRPOP =>
                    if( res.ok ) {
                        val arr = res.arr
                        if( arr != null && arr.size == 2 ) {
                            map.put("key",arr(0).value)
                            map.put("value",arr(1).value)
                        }
                    }
                case _ =>
                    code = ResultCodes.TLV_DECODE_ERROR
            }
            (code,map)
        }

        def decodeInternal(buf:ChannelBuffer):RedisResponse = {
            val len = buf.readableBytes()
            if ( len < 4) {
                return null;
            }

            var s = buf.readerIndex() 
            val max = s + len - 1

            val (valid,res) = decodeRecur(buf,s,max)
            res
        }

        def decodeRecur(buf:ChannelBuffer,s:Int,max:Int):Tuple2[Int,RedisResponse] = {

            val len = max - s + 1
            val ch = buf.getByte(s).toChar
            var e = findCrNl(buf,s,max)
            if( e < 0 ) return new Tuple2(-1,null)

            var valid = e - s + 1
            ch match {
                case '+' | ':' =>
                    val str = parseStr(buf,s+1,e-2)
                    return new Tuple2(valid,new RedisResponse(true,str))
                case '-' =>
                    val str = parseStr(buf,s+1,e-2)
                    return new Tuple2(valid,new RedisResponse(false,str))
                case '$' =>
                    val num = parseNumber(buf,s,e)
                    if( num < -1 ) 
                        return new Tuple2(-1,null)

                    if( num >= 0 ) {
                        if( len < valid + num + 2) return new Tuple2(-1,null)
                        val cr = buf.getByte(s+valid+num).toChar
                        val nl = buf.getByte(s+valid+num+1).toChar
                        if( cr != '\r' || nl != '\n' )
                            return new Tuple2(-1,null)
                        val str = parseStr(buf,s+valid,s+valid+num-1)
                        valid += num + 2
                        return new Tuple2(valid,new RedisResponse(true,str))
                    } else { // -1
                        return new Tuple2(valid,new RedisResponse(true,null))
                    }

                case '*' =>
                    val params = parseNumber(buf,s,e)
                    if( params < -1 )
                        return new Tuple2(-1,null)
                    if( params == -1 )
                        return new Tuple2(valid,new RedisResponse(true,null,null))
                    val arr = new ArrayBuffer[RedisResponse]()
                    var i = 0
                    var ts = e + 1
                    while( i < params ) {

                        val (v,res2) = decodeRecur(buf,ts,max)
                        if( v < 0 || res2 == null ) return new Tuple2(-1,null)
                        arr += res2

                        valid += v
                        ts += v

                        i += 1
                    }

                    return new Tuple2(valid,new RedisResponse(true,null,arr))
                case _ =>
                    return new Tuple2(-1,null)
            }

            return new Tuple2(-1,null)
        }

        // find last position of ...\r\n
        def findCrNl(buf:ChannelBuffer,min:Int,max:Int):Int = { 
            var i = min 
            while( i <= max ) {
                val ch = buf.getByte(i)
                if( ch == '\n' ) {
                    val lastbyte = buf.getByte(i-1)
                    if( lastbyte == '\r') {
                        return i
                    }
                }
                i += 1
            }
            -1
        }

        def parseStr(buf:ChannelBuffer,min:Int,max:Int):String = {
            val len = max - min + 1
            val bs = new Array[Byte](len)
            buf.getBytes(min,bs)
            new String(bs,"utf-8")
        }

        def parseNumber(buf:ChannelBuffer,min:Int,max:Int):Int = {
            var i = min 
            var s = ""
            while( i <= max ) {
                val ch = buf.getByte(i).toChar
                if( ch == '-' || ch >= '0' && ch <= '9' ) s += ch
                i += 1
            }
            try {
                s.toInt
            } catch {
                case e:Throwable =>
                    Int.MinValue
            }
        }

    }

}

