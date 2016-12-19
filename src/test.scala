package jvmdbbroker.core

import java.util.concurrent.atomic.{AtomicBoolean,AtomicInteger}
import java.io._
import scala.collection.mutable.{HashMap,ArrayBuffer,Buffer}
import scala.io.Source
import scala.xml._
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock
import java.text.SimpleDateFormat

class MockActor extends Actor with Logging with SyncedActor {

    val retmap = new ConcurrentHashMap[String,Response]()

    override def receive(v:Any) :Unit = {
        v match {
            case req: Request =>

                val buff = Router.main.mocks.getOrElse(req.serviceId+":"+req.msgId,null)
                if( buff == null ) {
                    reply(req,-10242504)
                    return
                }

                genResponse(req,buff)

            case _ =>
                log.error("unknown msg")
        }
    }

    def checkMatch(req:Request,cfg:MockCfg):Boolean =  {
        for( (k,v) <- cfg.req ) {
            v match {
                case s:String if s == "NULL" => 
                    if( req.s(k) != null ) return false
                case s:String => 
                    if( req.ns(k) != s ) return false
                case i:Int => 
                    if( !req.body.contains(k) ) return false
                    if( req.i(k) != i ) return false
                case _ => 
            }
        }
        true
    }

    def getCfg(req:Request,cfgs:ArrayBuffer[MockCfg]):MockCfg = {
        for( cfg <- cfgs ) {
            if( cfg.req.size == 0 ) return cfg
            if( checkMatch(req,cfg) ) return cfg
        }
        null
    }

    def genResponse(req:Request,cfgs:ArrayBuffer[MockCfg]) {
        val cfg = getCfg(req,cfgs)
        if( cfg == null ) {
            reply(req,-10242404)
            return
        }
        val code = cfg.res.i("$code")
        val params = HashMapStringAny()
        params ++= cfg.res
        if( code != 0 ) {
            reply(req,code)
            return
        }
        reply(req,0,params)
    }

    def reply(req:Request, code:Int) :Unit ={
        reply(req,code,new HashMapStringAny())
    }

    def reply(req:Request, code:Int, params:HashMapStringAny):Unit = {
        val (newbody,ec) = Router.main.encodeResponse(req.serviceId,req.msgId,code,params)
        var errorCode = code
        if( errorCode == 0 && ec != 0 ) {
            errorCode = ec
        }

        val res = new Response (errorCode,newbody,req)
        put(res.requestId,res)
    }

    def get(requestId:String): Response = {
        retmap.remove(requestId)
    }

    def put(requestId:String,ret: Response) {
        retmap.put(requestId,ret)
    }

}

class TestCaseV2Invoke(val tp:String, val service:String, val timeout:Int, val req:LinkedHashMapStringAny,val res:LinkedHashMapStringAny) {
    def toString(m:LinkedHashMapStringAny):String = {
        val b = new StringBuilder()
        for( (k,v) <- m ) {
            b.append(" ").append(k).append("=").append(v)
        }
        b.toString
    }
    def toString(indent:String):String = {
        var s = indent + "%s:%s timeout:%d req:%s res:%s".format(tp,service,timeout,toString(req),toString(res))
        s = s.replace("timeout:15000","")
        s
    }
}

class TestCaseV2(val tp:String,val name:String,val mocks:ArrayBuffer[TestCaseV2Invoke],val setups:ArrayBuffer[TestCaseV2Invoke],val teardowns:ArrayBuffer[TestCaseV2Invoke],val asserts:ArrayBuffer[TestCaseV2Invoke]) {
    override def toString():String = {

        val indent = "    "

        val buff = ArrayBufferString()

        if( name == "global")
            buff += "global:"
        else
            buff += "testcase:" + name

        if( mocks != null )
            mocks.foreach(buff += _.toString(indent))
        if( setups != null )
            setups.foreach(buff += _.toString(indent))
        if( teardowns != null )
            teardowns.foreach(buff += _.toString(indent))
        if( asserts != null )
            asserts.foreach(buff += _.toString(indent))

        buff.mkString("\n")
    }
}

object TestCaseRunner {

    val indent = "    "
    val codeTag = "$code"
    val f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val savedMocks = HashMap[String,ArrayBuffer[MockCfg]]()
    var sequence = new AtomicInteger(1)
    val lock = new ReentrantLock(false)
    val replied = lock.newCondition()
    var testCaseCount = 0
    var ir: InvokeResult = _

    var total = 0
    var success = 0
    var failed = 0

    def main(args:Array[String]) {

        var file = "."+File.separator+"testcase"+File.separator+"default.txt"
        var s = TestCaseRunnerV1.parseFile(args)
        if( s != "" ) file = s

        if(!isNewFormat(file)) {
            TestCaseRunnerV1.main(args)
            return
        }

        var dumpFlag = TestCaseRunnerV1.parseArg(args,"d")

        val lines = Source.fromFile(file,"UTF-8").getLines.toBuffer.map(removeComment).map(_.replace("\t"," ")).map(_.trim).filter( _.trim != "").filter( !_.startsWith("#") )
        val mergedlines = mergeLines(lines) 
        val (global,testcases) = parseFile(mergedlines)
        if( testcases == null ) {
            return
        }

        Main.mockMode = true
        Main.main(Array[String]())
        Router.main.mockMode = true
        Router.main.mockActor = new MockActor()

        println("-------------------------------------------")
        println("testcase file:  " + file)

        if( dumpFlag == "1" ) {
            println("-------------------------------------------")
            dump(global,testcases)
        }

        println("-------------------------------------------")
        runTest(global,testcases)

        println("testcase is over")
        println("-------------------------------------------")
        println("testcase total:%d, success:%d, failed:%d".format(total,success,failed))
        println("-------------------------------------------")
        Main.close()
    }

    def runTest(global:TestCaseV2,testcases:ArrayBuffer[TestCaseV2]) {
        Router.main.mocks.clear()
        if( global != null && global.mocks != null ) {
            for( m <- global.mocks ) {
                val (ok,msg) = installMock(m)
                if( !ok ) {
                    println("global mock install failed, stop test! service="+m.service+", reason="+msg)
                    return
                }
            }
        }

        saveGlobalMock()

        if( global != null && global.setups != null ) {
            for( i <- global.setups ) {
                val (ok,msg) = callServiceMustOk(i)
                if( !ok ) {
                    println("global setup failed, stop test! service="+i.service+", reason="+msg)
                    return
                }
            }
        }
        if( testcases != null ) {
            for( t <- testcases ) {
                doTest(t)
            }
        }
        if( global != null && global.teardowns != null ) {
            for( i <- global.teardowns ) {
                callServiceIgnoreResult(i)
            }
        }
    }

    def doTest(t:TestCaseV2) {
        Router.main.mocks.clear()
        Router.main.mocks ++= savedMocks

        if( t.mocks != null ) {
            for( m <- t.mocks ) {
                val (ok,msg) = installMock(m)
                if( !ok ) {
                    failed += 1
                    total += 1
                    println("testcase mock failed, testcase=%s, service=%s, reason=%s".format(t.name,m.service,msg))
                    return
                }
            }
        }
        if( t.setups != null ) {
            for( i <- t.setups ) {
                val (ok,msg) = callServiceMustOk(i)
                if( !ok ) {
                    failed += 1
                    total += 1
                    println("testcase setup failed, testcase=%s, service=%s, reason=%s".format(t.name,i.service,msg))
                    return
                }
            }
        }

        if( t.asserts != null ) {
            for( i <- t.asserts ) {
                val (ok,msg) = callServiceWithAssert(i)
                if( !ok ) {
                    failed += 1
                    total += 1
                    println("assert failed, testcase=%s, service=%s, reason=%s".format(t.name,i.service,msg))
                    return
                } else {
                    success += 1
                    total += 1
                }
            }
        }

        if( t.teardowns != null ) {
            for( i <- t.teardowns ) {
                callServiceIgnoreResult(i)
            }
        }
    }

    def callServiceIgnoreResult(i:TestCaseV2Invoke) {
        callService(i)
    }
    def callServiceMustOk(i:TestCaseV2Invoke):Tuple2[Boolean,String] = {
        callServiceWithAssert(i)
    }
    def callServiceWithAssert(i:TestCaseV2Invoke):Tuple2[Boolean,String] = {
        val ret = callService(i)
        val resMap = i.res.toHashMapStringAny()
        for( (k,v) <- resMap ) {
            v match {
                case s:String if s == "NULL" => 
                    if( ret.s(k) != null ) return (false,"["+k+"] not match, required:"+s+", actual:"+ret.s(k))
                case s:String => 
                    if( ret.ns(k) != s ) return (false,"["+k+"] not match, required:"+s+", actual:"+ret.ns(k))
                case i:Int => 
                    if( !ret.contains(k) ) return (false,"["+k+"] not match, required:"+i+", actual:not found")
                    if( ret.i(k) != i ) return (false,"["+k+"] not match, required:"+i+", actual:"+ret.i(k))
                case m:HashMapStringAny =>  // todo
                case a:ArrayBufferAny =>  // todo
                case _ => 
            }
        }
        (true,"success")
    }

    object TestActor extends Actor {
        def receive(v:Any) {
            v match {
                case res : InvokeResult =>
                    lock.lock();
                    try {
                       ir = res 
                       replied.signal()
                    } finally {
                        lock.unlock();
                    }
                case _ =>
                    println("unknown msg")
            }
        }
    }

    def generateSequence():Int = {
        sequence.getAndIncrement()
    }

    def callService(i:TestCaseV2Invoke):HashMapStringAny = {
            val params = i.req.toHashMapStringAny()
            val (serviceId,msgId) = Flow.router.serviceNameToId(i.service)
            if( serviceId == 0 || msgId == 0 ) {
                return HashMapStringAny("$code"-> (-10242405))
            }

            val (newbody,ec) = Flow.router.encodeRequest(serviceId, msgId, params)
            if( ec != 0 ) {
                return HashMapStringAny("$code"-> (-10242400))
            }

            val map = HashMapStringAny()
            lock.lock();
            try {
                val req = new Request (
                    "TEST"+RequestIdGenerator.nextId(),
                    "test:0",
                    generateSequence(),
                    1,
                    serviceId,
                    msgId,
                    newbody,
                    new HashMapStringAny(),
                    TestActor
                )

                ir = null
                ir = Router.main.send(req)

                if( ir == null ) {
                    replied.await( i.timeout, TimeUnit.MILLISECONDS )
                } 
                
                if( ir != null ) {
                    map ++= ir.res
                    map.put("$code",ir.code)
                } else {
                    map.put("$code",-10242504)
                }

            } finally {
                lock.unlock();
            }
            map
    }

    def saveGlobalMock() {
        for( (k,buff) <- Router.main.mocks ) {
            val newbuff = ArrayBuffer[MockCfg]()
            newbuff ++= buff
            savedMocks.put(k,newbuff)
        }
    }

    def installMock(m:TestCaseV2Invoke):Tuple2[Boolean,String] = {
        val service = m.service.toLowerCase
        val (serviceId,msgId) = Flow.router.serviceNameToId(service)
        if( serviceId == 0 || msgId == 0 ) {
            return (false,"mock failed, service not found")
        }
        val req = HashMapStringAny()
        val res = HashMapStringAny()
        if( m.req != null ) req ++= m.req
        if( m.res != null ) res ++= m.res
        val key = serviceId+":"+msgId
        var buff = Router.main.mocks.getOrElse(key,null)
        if( buff == null ) {
            buff = ArrayBuffer[MockCfg]()
            Router.main.mocks.put(key,buff)
        }
        buff += new MockCfg(key,req,res)
        (true,"success")
    }

    def dump(global:TestCaseV2,testcases:ArrayBuffer[TestCaseV2]) {
        if( global != null ) {
            println(global.toString())
            println()
        }
        for( t <- testcases ) {
            println(t.toString())
            println()
        }
    }

    def parseFile(lines:ArrayBufferString):Tuple2[TestCaseV2,ArrayBuffer[TestCaseV2]] = {
        var i = 0
        
        var global:TestCaseV2 = null
        var testcases  = ArrayBuffer[TestCaseV2]()

        while( i < lines.size ){
            val t = lines(i)
            t match {
                case t if t.startsWith("global:") =>
                    val (l_global,nextLine) = parseTestCase("global",lines,i)
                    global = l_global
                    i = nextLine
                case t if t.startsWith("testcase:") =>
                    val (l_testcase,nextLine) = parseTestCase("testcase",lines,i)
                    testcases += l_testcase
                    i = nextLine
                case _ =>
                    println("line not valid: " + t)
                    return null
            }
        }

        (global,testcases)
    }

    def parseTestCase(tp:String,lines:ArrayBufferString,start:Int):Tuple2[TestCaseV2,Int] = {

        testCaseCount += 1
        var name = parseAttr(lines(start),"testcase")
        if( name == "" ) name = "testcase_"+testCaseCount
        if( tp == "global" ) name = "global"
        var i = start + 1

        val mocks = ArrayBuffer[TestCaseV2Invoke]()
        val setups = ArrayBuffer[TestCaseV2Invoke]()
        val teardowns = ArrayBuffer[TestCaseV2Invoke]()
        val asserts = ArrayBuffer[TestCaseV2Invoke]()

        var over = false
        while( i < lines.size && !over ){
            val t = lines(i)
            t match {
                case t if t.startsWith("mock:") =>
                    mocks += parseInvoke("mock",lines,i)
                    i += 1
                case t if t.startsWith("setup:") =>
                    setups += parseInvoke("setup",lines,i)
                    i += 1
                case t if t.startsWith("teardown:") =>
                    teardowns += parseInvoke("teardown",lines,i)
                    i += 1
                case t if t.startsWith("assert:") =>
                    asserts += parseInvoke("assert",lines,i)
                    i += 1
                case t if t.startsWith("testcase:") =>
                    over = true
                case t if t.startsWith("global:") =>
                    over = true
                case _ =>
                    println("invalid line in testcase: " + t)
                    i += 1
            }
        }
        val o = new TestCaseV2(tp,name,mocks,setups,teardowns,asserts)
        (o,i)
    }

    def parseInvoke(tp:String,lines:ArrayBufferString,start:Int):TestCaseV2Invoke = {
        val line = lines(start)
        val service = parseAttr(line,tp)
        var timeout = parseAttr(line,"timeout")
        if( timeout == "" ) timeout = "15000"
        val req = parseReq(line)
        val res = parseRes(line)
        val reqMap = parseMap(req)
        val resMap = parseMap(res)
        if( tp != "mock" && !resMap.contains(codeTag) ) resMap.put(codeTag,0)
        new TestCaseV2Invoke(tp,service,timeout.toInt,reqMap,resMap)
    }

    val sep1 = 1.toChar.toString // blank
    val sep2 = 2.toChar.toString // =
    val sep3 = 3.toChar.toString // = in ""

    val r1 = """ (\$?[0-9a-zA-Z_]+)=""".r

    def parseMap(s:String):LinkedHashMapStringAny = {
        val map = new LinkedHashMapStringAny()

        var ns = replaceEqualInQuota(s)
        ns = r1.replaceAllIn(" "+ns,(m)=>sep1+m.group(1).replace("$","\\$")+sep2) // $0 ... $9 有特殊含义，$需转义
        val ss = ns.split(sep1).map(_.trim)
        for( t <- ss ) {
            val tt = t.split(sep2)
            val key = parseKey(tt(0))
            if( tt.size >= 2 ) map.put(key,parseValue(tt(1)))
            else if( tt.size >= 1 && key != "" ) map.put(key,"")
        }
        replaceFunction(map)
        map
    }

    def now():String={
        f.format(new java.util.Date())
    }

    def uuid(): String = {
        return java.util.UUID.randomUUID().toString().replaceAll("-", "")
    }

    def replaceFunction(map:LinkedHashMapStringAny) {
        for( (k,v) <- map ) {
            v match {
                case "$now" =>
                    map.put(k,now())
                case "$uuid" =>
                    map.put(k,uuid())
                case _ =>
            }
        }
    }

    def replaceEqualInQuota(s:String):String = {
        var inQuota = false
        var ts = ""
        for(c <- s) {

            if( inQuota && c == '=' ) {
                ts = ts + sep3
            } else if( c == '\"')  { 
                inQuota = !inQuota
                ts = ts + "\""
            } else {
                ts = ts + c
            }


        }
        ts
    }
    def reverseReplaceEqualInQuota(s:String):String = {
        s.replace(sep3,"=")
    }

    def parseKey(s:String):String = {
        val t = s.trim()
        t
    }

    def parseValue(s:String):Any = {
        val t = reverseReplaceEqualInQuota( s.trim() )
        if( t.length >= 2 && t.startsWith("\"") &&  t.endsWith("\"") ) return t.substring(1,t.length-1)
        if( t.startsWith("[") &&  t.endsWith("]") ) return JsonCodec.parseArrayNotNull(t)
        if( t.startsWith("{") &&  t.endsWith("}") ) return JsonCodec.parseObjectNotNull(t)
        if( t.startsWith("s:")) return t.substring(2)
        t
    }

    def parseReq(l:String):String = {
        val p1 = l.indexOf(" req:")
        if( p1 < 0 ) return ""
        val p2 = l.indexOf(" res:",p1+1)
        if( p2 < 0 ) return l.substring(p1+5)
        l.substring(p1+5,p2)
    }
    def parseRes(l:String):String = {
        val p1 = l.indexOf(" res:")
        if( p1 < 0 ) return ""
        l.substring(p1+5)
    }
    def parseAttr(l:String,field:String):String = {
        val p1 = l.indexOf(field+":")
        if( p1 < 0 ) return ""
        val p2 = l.indexOf(" ",p1+1)
        if( p2 < 0 ) return l.substring(p1+field.length+1)
        l.substring(p1+field.length+1,p2)
    }

    def mergeLines(lines:Buffer[String]):ArrayBufferString = {
        val buff = ArrayBufferString()
        for( i <- 0 until lines.size ){
            val t = lines(i)
            t match {
                case l if l.startsWith("global:") =>
                    buff += l
                case l if l.startsWith("testcase:") =>
                    buff += l
                case l if l.startsWith("mock:") =>
                    buff += l
                case l if l.startsWith("setup:") =>
                    buff += l
                case l if l.startsWith("teardown:") =>
                    buff += l
                case l if l.startsWith("assert:") =>
                    buff += l
                case _ =>
                    buff(buff.size-1) = buff(buff.size-1) + " " + t
            }
        }
        buff
    }

    def removeComment(line:String):String = {
        val p = line.lastIndexOf(" #")
        if( p >= 0 ) return line.substring(0,p)
        line
    }

    def isNewFormat(file:String):Boolean = {
        Source.fromFile(file,"UTF-8").getLines.map(_.trim).filter( l => l.startsWith("testcase:") || l.startsWith("global:")).size > 0
    }

}

class TestCaseV1(val serviceId:Int,val msgId:Int,val body:HashMapStringAny,val repeat:Int = 1,val xhead:HashMapStringAny = new HashMapStringAny() )

object TestCaseRunnerV1 {

    var requestCount = 1
    var replyCount = 0
    var soc : SocImpl = _

    val lock = new ReentrantLock(false)
    val replied = lock.newCondition()

    def main(args:Array[String]) {

        var max = 2000000
        var s = parseArg(args,"maxPackageSize")
        if( s != "" ) max = s.toInt

        var file = "."+File.separator+"testcase"+File.separator+"default.txt"
        s = parseFile(args)
        if( s != "" ) file = s
        println()
        println("testcase file:  " + file)

        val codecs = new TlvCodecs("."+File.separator+"avenue_conf")

        val in = new InputStreamReader(new FileInputStream("."+File.separator+"config.xml"),"UTF-8")
        val cfgXml = XML.load(in)
        val port = (cfgXml \ "SapPort").text.toInt
        val timeout = 15000
        var serverAddr = "127.0.0.1:"+port
        s = (cfgXml \ "TestServerAddr").text
        if( s != "" ) serverAddr = s

        soc = new SocImpl(serverAddr,codecs,callback,connSizePerAddr=1,maxPackageSize=max)

        val lines = Source.fromFile(file,"UTF-8").getLines.toList.filter( _.trim != "").filter( !_.startsWith("#") )

        val testcases = parseLines(lines)

        //val sendCount = testcases.foldLeft(0) { (sum,tc) => sum + tc.repeat }

        println("total test case: "+testcases.size)

        var seq = 0

        for( i <- 1 to testcases.size ) {

            val t = testcases(i-1)

            lock.lock();
            try {
                for( j <- 1 to t.repeat ) {
                    seq += 1

                    replyCount = 0

                    val req = new SocRequest(seq.toString,t.serviceId,t.msgId,t.body,AvenueCodec.ENCODING_UTF8,t.xhead)
                    println("req="+req)
                    soc.send(req,timeout)
                    if( replyCount < requestCount )
                        replied.await( timeout + 100, TimeUnit.MILLISECONDS )

                }
            } finally {
                lock.unlock();
            }

        }

        soc.close
    }

    def callback(any:Any){

        any match {

            case reqRes: SocRequestResponseInfo =>

                lock.lock();
                try {
                    val reqRes = any.asInstanceOf[SocRequestResponseInfo]
                    println("res="+reqRes.res)
                    replyCount += 1
                    if( replyCount >= requestCount )
                        replied.signal()
                } finally {
                    lock.unlock();
                }

            case ackInfo: SocRequestAckInfo =>

                println("ack="+ackInfo.req.requestId)

            case _ =>
        }
    }
    def parseLines(lines: List[String]) : ArrayBuffer[TestCaseV1] = {

        val testcases = new ArrayBuffer[TestCaseV1]()

        for( line <- lines ) {
            val tc = parseLine(line)
            if( tc != null )
                testcases += tc
        }

        testcases
    }

    def parseArg(args:Array[String],key:String):String = {
        for(i <- 0 until args.size) {
            if( args(i) == "--" + key && i + 1 < args.size ) {
                return args(i+1)
            }
            if( args(i) == "-" + key ) {
                return "1"
            }
        }
        ""
    }

    def parseFile(args:Array[String]):String = {
        var i = 0
        while(i < args.size) {
            if( args(i).startsWith("--") ) i += 2
            else if( args(i).startsWith("-") ) i += 1
            else return args(i)
        }
        ""
    }

    def parseLine(line:String) : TestCaseV1 = {

        val p1 = line.indexOf(",")
        if( p1 <= 0 ) return null

        val p2 = line.indexOf(",",p1+1)
        if( p2 <= 0 ) return null

        val serviceId = line.substring(0,p1).toInt
        val msgId = line.substring(p1+1,p2).toInt
        val json = line.substring(p2+1)

        val body = JsonCodec.parseObject(json)

        var repeat = body.i("x_repeat")
        if( repeat == 0 ) repeat = 1

        val xhead = HashMapStringAny()
        val socId = body.s("x_socId","")
        if( socId != "") xhead.put("socId",socId)
        val spsId = body.s("x_spsId","")
        if( spsId != "") xhead.put("spsId",spsId)
        val uniqueId = body.s("x_uniqueId","")
        if( uniqueId != "") xhead.put("uniqueId",uniqueId)
        val appId = body.s("x_appId","")
        if( appId != "") xhead.put("appId",appId)

        val tc = new TestCaseV1(serviceId,msgId,body,repeat,xhead)
        tc
    }

}

