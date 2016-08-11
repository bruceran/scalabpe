package jvmdbbroker.core

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import org.jboss.netty.util._;

import scala.collection.mutable.{ArrayBuffer,HashMap,SynchronizedQueue}
import scala.xml._

class SocActor(val router: Router,val cfgNode: Node) extends Actor with RawRequestActor with Logging with Closable with SelfCheckLike  with Dumpable  {

    var timeout = 30000

    var serviceIds: String = _

    val queueSize = 20000
    var maxThreadNum = 2
    var threadFactory : ThreadFactory = _
    var pool : ThreadPoolExecutor = _

    var socWrapper: SocImpl = _

    init

    def dump() {

        log.info("--- serviceIds="+serviceIds)

        val buff = new StringBuilder

        buff.append("pool.size=").append(pool.getPoolSize).append(",")
        buff.append("pool.getQueue.size=").append(pool.getQueue.size).append(",")

        log.info(buff.toString)

        socWrapper.dump
    }

    def init() {

        serviceIds = (cfgNode \ "ServiceId").text
        val addrs = (cfgNode \ "ServerAddr").map(_.text).toList.mkString(",")

        var s = (cfgNode \ "@threadNum").text
        if( s != "" ) maxThreadNum = s.toInt

        s = (cfgNode \ "@timeout").text
        if( s != "" ) timeout = s.toInt

        var retryTimes = 2
        s = (cfgNode \ "@retryTimes").text
        if( s != "" ) retryTimes = s.toInt

        var connectTimeout = 15000
        s = (cfgNode \ "@connectTimeout").text
        if( s != "" ) connectTimeout = s.toInt

        var pingInterval = 60000
        s = (cfgNode \ "@pingInterval").text
        if( s != "" ) pingInterval = s.toInt

        var maxPackageSize = 2000000
        s = (cfgNode \ "@maxPackageSize").text
        if( s != "" ) maxPackageSize = s.toInt

        var connSizePerAddr = 8
        s = (cfgNode \ "@connSizePerAddr").text
        if( s != "" ) connSizePerAddr = s.toInt

        var timerInterval  = 100
        s = (cfgNode \ "@timerInterval").text
        if( s != "" ) timerInterval = s.toInt

        var reconnectInterval  = 1
        s = (cfgNode \ "@reconnectInterval").text
        if( s != "" ) reconnectInterval = s.toInt

        val firstServiceId = serviceIds.split(",")(0)
        threadFactory = new NamedThreadFactory("soc"+firstServiceId)
        pool = new ThreadPoolExecutor(maxThreadNum, maxThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize),threadFactory)
        pool.prestartAllCoreThreads()

        var isSps = router.getConfig("isSps","0") == "1"
        var reportSpsTo = router.getConfig("spsReportTo","55605:1")
        val reportSpsServiceId = reportSpsTo.split(":")(0)
        if( !serviceIds.split(",").contains(reportSpsServiceId) ) reportSpsTo="0:0"

        socWrapper = new SocImpl(addrs,router.codecs,this.receive,
            retryTimes,connectTimeout,pingInterval,maxPackageSize,
            connSizePerAddr,timerInterval,reconnectInterval,
            isSps,reportSpsTo,
            actor = this)

        log.info("SocActor started {}",serviceIds)
    }

    def close() {

        val t1 = System.currentTimeMillis

        pool.shutdown()

        pool.awaitTermination(5,TimeUnit.SECONDS)

        val t2 = System.currentTimeMillis
        if( t2 - t1 > 100 )
            log.warn("SocActor long time to shutdown pool, ts={}",t2-t1)


        socWrapper.close()
        log.info("SocActor stopped {}",serviceIds)
    }

    override def receive(v:Any) :Unit = {

        try {
            pool.execute( new Runnable() {
                def run() {
                    try {
                        onReceive(v)
                    } catch {
                        case e:Exception =>
                            log.error("soc exception v={}",v,e)
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                // ignore the message
                log.error("soc queue is full, serviceIds={}",serviceIds)
        }
    }

    def onReceive(v:Any) :Unit = {

        v match {

            case req: Request =>

                socWrapper.send(req,timeout)

            case reqResInfo: RequestResponseInfo =>

                router.reply(reqResInfo)

            case reqAckInfo: RequestAckInfo =>

                router.receiveAck(reqAckInfo)

                // raw request/response 

            case rawReq: RawRequest =>

                if( rawReq.sender eq this )
                    router.send(rawReq)
                else
                    socWrapper.send(rawReq,timeout)

            case rawRes: RawResponse =>

                socWrapper.sendResponse(rawRes.data,rawRes.connId)

            case reqResInfo : RawRequestResponseInfo =>

                if( reqResInfo.rawReq.sender eq this ){
                    socWrapper.sendResponse(reqResInfo.rawRes.data,reqResInfo.rawRes.connId)
                }else{
                    router.reply(reqResInfo)
                }

                //case t: RawRequestErrorResponse =>

                //socWrapper.sendErrorCode(t.rawReq,t.code)

            case reqAckInfo : RawRequestAckInfo =>

                if( reqAckInfo.rawReq.sender eq this )
                    socWrapper.sendAck(reqAckInfo.rawReq)
                else
                    router.receiveAck(reqAckInfo)

            case _ =>

                log.error("unknown msg")

        }
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = socWrapper.selfcheck()
        buff
    }

}

// used by user
trait Soc{
    def send(rawReq: RawRequest, timeout: Int):Unit;
    def send(req: Request, timeout: Int):Unit;
    def send(socReq: SocRequest, timeout: Int):Unit;
}

class SocImpl(
    val addrs: String,
    val codecs: TlvCodecs,
    val receiver_f: (Any)=>Unit,
    val retryTimes : Int = 2,
    val connectTimeout :Int = 15000,
    val pingInterval: Int = 60000,
    val maxPackageSize: Int = 2000000,
    val connSizePerAddr: Int = 8 ,
    val timerInterval :Int = 100,
    val reconnectInterval : Int = 1,
    val isSps: Boolean = false,
    val reportSpsTo: String = "0:0",
    val bossExecutor:ThreadPoolExecutor = null,
    val workerExecutor:ThreadPoolExecutor = null,
    val timer : HashedWheelTimer = null,
    val qte : QuickTimerEngine = null,
    val waitForAllConnected : Boolean = false,
    val waitForAllConnectedTimeout :Int = 60000,
    val connectOneByOne :Boolean = false,
    val reuseAddress: Boolean = false,
    val startPort: Int = -1,
    val actor: Actor = null
) extends Soc with Soc4Netty with Logging with Dumpable {

    val EMPTY_BUFFER = ByteBuffer.allocate(0)
    var nettyClient : NettyClient = _
    val generator = new AtomicInteger(1)
    val converter = new AvenueCodec
    val keyMap = new ConcurrentHashMap[String,String]()
    val dataMap = new ConcurrentHashMap[Int,CacheData]()

    init

    def dump() {

        val buff = new StringBuilder

        buff.append("dataMap.size=").append(dataMap.size).append(",")

        log.info(buff.toString)

        nettyClient.dump
    }

    def init() {

        nettyClient = new NettyClient(this,
            addrs,
            connectTimeout,
            pingInterval,
            maxPackageSize,
            connSizePerAddr,
            timerInterval,
            reconnectInterval,
            bossExecutor,
            workerExecutor,
            timer,
            qte,
            waitForAllConnected,
            waitForAllConnectedTimeout,
            connectOneByOne,
            reuseAddress,
            startPort, 
            isSps
            )

        log.info("soc {} started",addrs)
    }

    def close() {
        nettyClient.close()
        log.info("soc {} stopped",addrs)
    }

    def selfcheck() : ArrayBuffer[SelfCheckResult] = {
        val buff = nettyClient.selfcheck()
        buff
    }

    def send(data:AvenueData,timeout:Int):Int = {
        try {

            val buff = converter.encode(data)

            var ok = nettyClient.send(data.sequence,buff,timeout)
            if( ok ) 0 else ResultCodes.SOC_NOCONNECTION
        } catch {
            case e:Throwable =>
                log.error("send exception",e)
                ResultCodes.TLV_ENCODE_ERROR
        }
    }

    def sendByAddr(data:AvenueData,timeout:Int,addr:String):Int = {
        try {

            val buff = converter.encode(data)

            var ok = nettyClient.sendByAddr(data.sequence,buff,timeout,addr)
            if( ok ) 0 else ResultCodes.SOC_NOCONNECTION
        } catch {
            case e:Throwable =>
                log.error("send exception",e)
                ResultCodes.TLV_ENCODE_ERROR
        }
    }

    def sendByConnId(data:AvenueData,timeout:Int,connId:String):Int = {
        try {

            var key : String = null
            if( connId != null && connId != "" )
                key = keyMap.get(connId)

            val buff = converter.encode(data,key)

            var ok = nettyClient.sendByConnId(data.sequence,buff,timeout,connId)
            if( ok ) 0 else ResultCodes.SOC_NOCONNECTION
        } catch {
            case e:Throwable =>
                log.error("send exception",e)
                ResultCodes.TLV_ENCODE_ERROR
        }
    }

    def sendAck(rawReq:RawRequest):Int = {
        val data = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            rawReq.data.sequence,
            0,
            rawReq.data.encoding,
            AvenueCodec.ACK_CODE,
            EMPTY_BUFFER,
            EMPTY_BUFFER )

        sendResponse(data,rawReq.connId)
    }

    def sendErrorCode(rawReq:RawRequest,code:Int):Int = {
        val data = new AvenueData (
            AvenueCodec.TYPE_RESPONSE,
            rawReq.data.serviceId,
            rawReq.data.msgId,
            rawReq.data.sequence,
            0,
            rawReq.data.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER )

        sendResponse(data,rawReq.connId)
    }

    def sendResponse(data:AvenueData,connId:String):Int = {

        if( connId == null || connId == "" )
            return ResultCodes.SOC_NOCONNECTION

        val key = keyMap.get(connId)

        try {
            val buff = converter.encode(data,key)
            val ok = nettyClient.sendResponse(data.sequence,buff,connId)
            if( ok ) 0 else ResultCodes.SOC_NOCONNECTION
        } catch {
            case e:Throwable =>
                ResultCodes.TLV_ENCODE_ERROR
        }
    }

    def receive(res:ByteBuffer, connId:String):Tuple2[Boolean,Int] = {

        var data : AvenueData = null

        var key : String = null
        if( connId != null && connId != "" )
            key = keyMap.get(connId)

        try {
            data = converter.decode(res,key)
        } catch {
            case e:Throwable =>
                log.error("decode exception")
                val ret = (false,0)
                return ret
        }

        data.flag match {

            case AvenueCodec.TYPE_REQUEST =>

                // append remote addr to xhead, the last addr is always remote addr
                try {
                    data.xhead = TlvCodec4Xhead.appendGsInfo(data.xhead,parseRemoteAddr(connId))
                } catch {
                    case e:Throwable =>
                }

                try {
                    receive(SosRequest(data,connId))
                } catch {
                    case e:Throwable =>
                        log.error("receive exception res={}",data,e)
                }

                val ret = (false,0)
                return ret

            case AvenueCodec.TYPE_RESPONSE =>

                if( isPong(data.serviceId, data.msgId)  ) {
                    val ret = (false,0)
                    return ret
                }

                if( isAck(data.code)  ) {

                    try {
                        receive(SocSendAck(data,connId))
                    } catch {
                        case e:Throwable =>
                            log.error("receive exception res={}",data,e)
                    }

                    val ret = (false,0)
                    return ret
                }

                try {
                    receive(SocSendResponse(data,connId))
                } catch {
                    case e:Throwable =>
                        log.error("receive exception res={}",data,e)
                        val ret = (true, data.sequence)
                        return ret
                }

            case _ =>
                log.error("unknown type")

        }

        val ret = (true, data.sequence)
        return ret
    }

    def networkError(sequence:Int,connId:String) {
        try {
            receive(SocSendNetworkError(sequence,connId))
        } catch {
            case e:Throwable =>
                log.error("networkError callback exception")
        }
    }

    def timeoutError(sequence:Int,connId:String) {

        try {
            receive(SocSendTimeout(sequence,connId))
        } catch {
            case e:Throwable =>
                log.error("timeoutError callback exception")
        }

    }

    def generatePing():ByteBuffer = {

        val seq = generateSequence()

        val res = new AvenueData (
            AvenueCodec.TYPE_REQUEST,
            0,
            0,
            seq,
            0,
            0,
            0,
            EMPTY_BUFFER,
            EMPTY_BUFFER )

        val bb = converter.encode(res)
        bb
    }

    def generateReportSpsId():ByteBuffer = {

        if( reportSpsTo == "0:0" ) return null

        val seq = generateSequence()

        val xhead = HashMapStringAny(AvenueCodec.KEY_SPS_ID->TlvCodec4Xhead.SPS_ID_0)
        val reportSpsInfo = reportSpsTo.split(":")
        val xheadbuff = TlvCodec4Xhead.encode(reportSpsInfo(0).toInt,xhead)
        val res = new AvenueData (
            AvenueCodec.TYPE_REQUEST,
            reportSpsInfo(0).toInt,
            reportSpsInfo(1).toInt,
            seq,
            0,
            0,
            0,
            xheadbuff,
            EMPTY_BUFFER )

        val bb = converter.encode(res)
        bb
    }

    def isPong(serviceId:Int,msgId:Int) = { serviceId == 0 && msgId == 0 }
    def isAck(code:Int) = { code == AvenueCodec.ACK_CODE }

    def generateSequence():Int = {
        generator.getAndIncrement()
    }

    def send(rawReq: RawRequest, timeout: Int):Unit = {

        val req = rawReq.data

        val sequence = generateSequence()
        val data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            req.serviceId,
            req.msgId,
            sequence,
            req.mustReach,
            req.encoding,
            req.code,
            req.xhead, req.body )
        dataMap.put(sequence,new CacheData(rawReq,timeout))

        val ret = send(data,timeout)

        if(ret != 0) {
            dataMap.remove(sequence)
            val rawRes = createErrorResponse(ret,rawReq)
            receiver_f(new RawRequestResponseInfo(rawReq,rawRes) )
        }
    }

    def send(req: Request, timeout: Int):Unit = {

        val tlvCodec = codecs.findTlvCodec(req.serviceId)
        if( tlvCodec == null ) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR,req)
            receiver_f(new RequestResponseInfo(req,res))
            return
        }

        val sequence = generateSequence()
        val xhead = TlvCodec4Xhead.encode(req.serviceId,req.xhead)
        val (body,ec) = tlvCodec.encodeRequest(req.msgId,req.body,req.encoding)
        if( ec !=  0 ) {
            log.error("encode request error, serviceId="+req.serviceId+", msgId="+req.msgId)

            val res = createErrorResponse(ec,req)
            receiver_f(new RequestResponseInfo(req,res))
            return
        }

        val data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            req.serviceId,
            req.msgId,
            sequence,
            0,
            req.encoding,
            0,
            xhead, body )
        dataMap.put(sequence,new CacheData(req,timeout))

        var ret = 0
        if( req.toAddr == null )
            ret = send(data,timeout)
        else
            ret = sendByAddr(data,timeout,req.toAddr)

        if(ret != 0 ) {
            dataMap.remove(sequence)
            val res = createErrorResponse(ret,req)
            receiver_f(new RequestResponseInfo(req,res))
        }

    }

    def send(req: SocRequest, timeout: Int):Unit = {

        val tlvCodec = codecs.findTlvCodec(req.serviceId)
        if( tlvCodec == null ) {
            val res = createErrorResponse(ResultCodes.TLV_ENCODE_ERROR,req)
            receiver_f(new SocRequestResponseInfo(req,res))
            return
        }

        val sequence = generateSequence()

        val xhead = TlvCodec4Xhead.encode(req.serviceId,req.xhead)

        val (body,ec) = tlvCodec.encodeRequest(req.msgId,req.body,req.encoding)
        if( ec != 0 ) {
            log.error("encode request error, serviceId="+req.serviceId+", msgId="+req.msgId)
            val res = createErrorResponse(ec,req)
            receiver_f(new SocRequestResponseInfo(req,res))
            return
        }

        val data = new AvenueData(
            AvenueCodec.TYPE_REQUEST,
            req.serviceId,
            req.msgId,
            sequence,
            0,
            req.encoding,
            0,
            xhead, body )
        dataMap.put(sequence,new CacheData(req,timeout))

        if( "select_channel_first" == req.connId ) {
            req.connId = nettyClient.selectChannel()
            if( req.connId == null ) {
                log.error("no channel found")
                val res = createErrorResponse(ResultCodes.SOC_NOCONNECTION,req)
                receiver_f(new SocRequestResponseInfo(req,res))
                return
            }
            // log.info("channel found, channelId="+req.connId)
        }

        var ret = 0
        if( req.connId == null )
            ret = send(data,timeout)
        else
            ret = sendByConnId(data,timeout,req.connId)

        if(ret != 0 ) {
            dataMap.remove(sequence)
            val res = createErrorResponse(ret,req)
            receiver_f(new SocRequestResponseInfo(req,res))
        }

    }

    def createErrorResponse(code:Int,rawReq:RawRequest):RawResponse = {
        val data = rawReq.data
        val res = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            data.serviceId,
            data.msgId,
            data.sequence,
            0,
            data.encoding,
            code,
            EMPTY_BUFFER,EMPTY_BUFFER)
        val rawRes = new RawResponse(res,rawReq)
        rawRes
    }

    def createErrorResponse(code:Int,req:Request):Response = {
        val res = new Response (code,new HashMapStringAny(),req)
        res
    }

    def createErrorResponse(code:Int,req:SocRequest):SocResponse = {
        val res = new SocResponse (req.requestId,code,new HashMapStringAny())
        res
    }

    def receive(v:Any):Unit = {

        v match {

            case SocSendResponse(data,connId) =>

                val saved = dataMap.remove(data.sequence)
                if( saved != null ) {
                    saved.data match {

                        case rawReq: RawRequest =>

                            val res = new AvenueData(
                                AvenueCodec.TYPE_RESPONSE,
                                rawReq.data.serviceId,
                                rawReq.data.msgId,
                                rawReq.data.sequence,
                                0,
                                rawReq.data.encoding,
                                data.code,
                                EMPTY_BUFFER,data.body)
                            val rawRes = new RawResponse(res,rawReq)
                            rawRes.remoteAddr = parseRemoteAddr(connId)
                            receiver_f(new RawRequestResponseInfo(rawReq,rawRes))

                        case req: Request =>


                            val tlvCodec = codecs.findTlvCodec(req.serviceId)
                            if( tlvCodec != null ) {

                                val (body,ec) = tlvCodec.decodeResponse(req.msgId,data.body,data.encoding)
                                var errorCode = data.code 
                                if( errorCode == 0 && ec != 0 ) {
                                    log.error("decode response error, serviceId="+req.serviceId+", msgId="+req.msgId)

                                    errorCode = ec
                                }

                                val res = new Response (errorCode,body,req)
                                res.remoteAddr = parseRemoteAddr(connId)
                                receiver_f(new RequestResponseInfo(req,res))
                            }

                        case req: SocRequest =>

                            val tlvCodec = codecs.findTlvCodec(req.serviceId)
                            if( tlvCodec != null ) {

                                val (body,ec) = tlvCodec.decodeResponse(req.msgId,data.body,data.encoding)
                                var errorCode = data.code 
                                if( errorCode == 0 && ec != 0 ) {
                                    log.error("decode response error, serviceId="+req.serviceId+", msgId="+req.msgId)
                                    errorCode = ec
                                }

                                val res = new SocResponse (req.requestId,errorCode,body)
                                res.remoteAddr = parseRemoteAddr(connId)
                                res.connId = connId
                                receiver_f(new SocRequestResponseInfo(req,res))
                            }
                        }
                    } else {
                        log.warn("receive but sequence not found, seq={}",data.sequence)
                    }

            case SocSendAck(data,connId) =>

                val saved = dataMap.get(data.sequence) // donot remove
                if( saved != null ) {
                    saved.data match {

                        case rawReq: RawRequest =>
                            receiver_f(new RawRequestAckInfo(rawReq))
                        case req: Request =>
                            receiver_f(new RequestAckInfo(req))
                        case req: SocRequest =>
                            receiver_f(new SocRequestAckInfo(req))
                    }

                } else {
                    log.warn("receive but sequence not found, seq={}",data.sequence)
                }

            case SocSendTimeout(sequence,connId) =>
                val saved = dataMap.remove(sequence)
                if( saved != null ) {
                    saved.data match {

                        case rawReq: RawRequest =>
                            val rawRes = createErrorResponse(ResultCodes.SOC_TIMEOUT,rawReq)
                            rawRes.remoteAddr = parseRemoteAddr(connId)
                            receiver_f(new RawRequestResponseInfo(rawReq,rawRes))
                        case req: Request =>
                            val res = createErrorResponse(ResultCodes.SOC_TIMEOUT,req)
                            res.remoteAddr = parseRemoteAddr(connId)
                            receiver_f(new RequestResponseInfo(req,res))
                        case req: SocRequest =>
                            val res = createErrorResponse(ResultCodes.SOC_TIMEOUT,req)
                            res.remoteAddr = parseRemoteAddr(connId)
                            receiver_f(new SocRequestResponseInfo(req,res))

                    }
                    } else {
                        log.error("timeout but sequence not found, seq={}",sequence)
                    }

            case SocSendNetworkError(sequence,connId) =>
                val saved = dataMap.remove(sequence)
                if( saved != null ) {

                    saved.sendTimes += 1
                    val now = System.currentTimeMillis
                    if( saved.sendTimes >= retryTimes || now + 30 >= saved.sendTime + saved.timeout ) {

                        saved.data match {

                            case rawReq: RawRequest =>
                                val rawRes = createErrorResponse(ResultCodes.SOC_NETWORKERROR,rawReq)
                                rawRes.remoteAddr = parseRemoteAddr(connId)
                                receiver_f(new RawRequestResponseInfo(rawReq,rawRes))
                            case req: Request =>
                                val res = createErrorResponse(ResultCodes.SOC_NETWORKERROR,req)
                                res.remoteAddr = parseRemoteAddr(connId)
                                receiver_f(new RequestResponseInfo(req,res))
                            case req: SocRequest =>
                                val res = createErrorResponse(ResultCodes.SOC_NETWORKERROR,req)
                                res.remoteAddr = parseRemoteAddr(connId)
                                receiver_f(new SocRequestResponseInfo(req,res))
                        }
                    } else {

                            log.warn("resend data, req={},sendTimes={}",saved.data,saved.sendTimes)

                            saved.data match {
                                case rawReq: RawRequest =>
                                    send(rawReq,saved.timeout)
                                case req: Request =>
                                    send(req,saved.timeout)
                                case req: SocRequest =>
                                    send(req,saved.timeout)
                            }

                    }
                } else {
                    log.error("network error but sequence not found, seq={}",sequence)
                }

            case SosRequest(data,connId) =>

                if( actor == null ) {
                    val tlvCodec = codecs.findTlvCodec(data.serviceId)
                    if( tlvCodec != null ) {

                        val requestId = RequestIdGenerator.nextId()

                        val (body,ec) = tlvCodec.decodeRequest(data.msgId,data.body,data.encoding)
                        if( ec != 0 ) {
                            log.error("decode request error, serviceId="+data.serviceId+", msgId="+data.msgId)
                            val res = new Response(requestId,
                                connId,
                                data.sequence,
                                data.encoding,
                                data.serviceId,
                                data.msgId,
                                ec,
                                HashMapStringAny(),
                                null)
                            send(res)
                            return
                        }

                        val req = new Request(requestId,
                            connId,
                            data.sequence,
                            data.encoding,
                            data.serviceId,
                            data.msgId,
                            new HashMapStringAny(),
                            body,
                            null)
                        receiver_f(req)
                    } else {
                        log.warn("serviceId not found, serviceId={}",data.serviceId)
                    }
                } else {

                    val requestId = RequestIdGenerator.nextId()
                    val rawReq = new RawRequest(requestId,data,connId,actor)
                    receiver_f(rawReq)

                }

        }
    }

    def send(res: Response):Unit = {

        val tlvCodec = codecs.findTlvCodec(res.serviceId)
        if( tlvCodec == null ) {
            log.error("response serviceId not found")
            return
        }

        val (body,ec) = tlvCodec.encodeResponse(res.msgId,res.body,res.encoding)
        var errorCode = res.code
        if( errorCode == 0 && ec != 0 ) {
            log.error("encode response error, serviceId="+res.serviceId+", msgId="+res.msgId)
            errorCode = ec
        }

        val data = new AvenueData(
            AvenueCodec.TYPE_RESPONSE,
            res.serviceId,
            res.msgId,
            res.sequence,
            0,
            res.encoding,
            errorCode,
            EMPTY_BUFFER,
            body )

        val ret = sendResponse(data,res.connId)
        if(ret != 0 ) {
            log.error("send response error")
        }

    }

    def parseRemoteAddr(connId:String):String = {

        val p = connId.lastIndexOf(":")

        if (p >= 0)
            connId.substring(0,p)
        else
            "0.0.0.0:0"
    }

    case class SocSendAck(data:AvenueData,connId:String)
    case class SocSendResponse(data:AvenueData,connId:String)
    case class SocSendTimeout(sequence:Int,connId:String)
    case class SocSendNetworkError(sequence:Int,connId:String)
    case class SosRequest(data:AvenueData,connId:String)

    class CacheData(val data:Any, val sendTime:Long, val timeout:Int, var sendTimes:Int) {
        def this(rawReq: RawRequest,timeout:Int) { this(rawReq,System.currentTimeMillis,timeout,0) }
        def this(req: Request,timeout:Int) { this(req,System.currentTimeMillis,timeout,0) }
        def this(socReq: SocRequest,timeout:Int) { this(socReq,System.currentTimeMillis,timeout,0) }
    }

}
