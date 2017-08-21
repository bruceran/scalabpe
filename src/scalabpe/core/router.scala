package scalabpe.core
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.StringReader
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.reflect.runtime.universe
import scala.xml.Elem
import scala.xml.XML

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

import org.apache.commons.io.FileUtils

object Router {
    val DO_NOT_REPLY: String = ""
    var profile = "default"
    var configXml = "config.xml"
    var parameterXml = "parameter.xml"
    var dataDir = ""
    var tempDir = ""
    var testMode = false // 单元测试模式下运行
    var main: Router = null
}

object FlowTimoutType {
    val TYPE_CALL = 1
    val TYPE_FLOW = 2
}

class MockCfg(val serviceidmsgid: String, val req: HashMapStringAny, val res: HashMapStringAny)

class Router(val rootDir: String, val startSos: Boolean = true, var mockMode: Boolean = false) extends Logging with Closable with Dumpable {

    val avenueConfDir = rootDir + File.separator + "avenue_conf"
    val pluginConfFile = "scalabpe.plugins.conf"
    val EMPTY_BUFFER = ChannelBuffers.buffer(0)

    val pReg = """@[0-9a-zA-Z_-]+[ /<\]"]""".r // allowed: @xxx< @xxx]]> @xxx[SPACE]
    val pReg2 = """@[0-9a-zA-Z_-]+$""".r // allowed: @xxx
    val pRegName = """^(@[0-9a-zA-Z_-]+).*$""".r

    val actorMap = HashMap[String, Actor]()
    val allActors = new ArrayBuffer[Actor]()
    val allReqFilters = new ArrayBuffer[RequestFilter]()
    val allResFilters = new ArrayBuffer[ResponseFilter]()
    val allBeans = new ArrayBuffer[Bean]()

    val serviceIdsAllowed = HashSet[Int]() // to control the visibility of not-flow serviceids, default is closed
    val serviceIdsNotAllowed = HashSet[Int]() // to control the visibility of flow serviceids, default is open
    val reverseServiceIds = HashSet[Int]() // 3 or other service for scalabpe clients

    val mocks = HashMap[String, ArrayBuffer[MockCfg]]()
    var mockActor: Actor = null

    var mustReachEnabled = true

    val qte = new QuickTimerEngine(timeout_function, 25)

    var globalCls = "scalabpe.flow.Global"
    var codecs: TlvCodecs = _
    var cfgParameters = HashMapStringString()
    var cfgXml: Elem = _
    var asyncLogActor: Actor = _
    var mustReachActor: MustReachActor = _
    var sos: Sos = _
    var regdishooks: ArrayBuffer[RegDisHook] = _

    val parameters = new HashMapStringString()

    val started = new AtomicBoolean()
    val shutdown = new AtomicBoolean()

    init

    def callGlobalInit() {
        val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

        try {
            val module = runtimeMirror.staticModule(globalCls)
            val obj = runtimeMirror.reflectModule(module)
            val method = obj.instance.getClass.getMethod("init")
            method.invoke(obj.instance)
        } catch {
            case e: Throwable =>
                log.info(globalCls + ".init not found")
        }
    }

    def callGlobalClose() {
        val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

        try {
            val module = runtimeMirror.staticModule(globalCls)
            val obj = runtimeMirror.reflectModule(module)
            val method = obj.instance.getClass.getMethod("close")
            method.invoke(obj.instance)
        } catch {
            case e: Throwable =>
                log.info(globalCls + ".close not found")
        }
    }

    def init() {

        try {
            initInternal()

            sos.start()

            started.set(true)

        } catch {
            case e: Exception =>
                log.error("router init exception", e)
                close()
                throw e
        }

    }

    def close() {
        var dumpFlag = true

        if (cfgXml != null) {
            var s = (cfgXml \ "DumpBeforeClose").text
            if (s != "") {
                dumpFlag = (s == "true" || s == "yes" || s == "t" || s == "y" || s == "1")
            }
        }

        if (dumpFlag) {
            log.info("-------begin dump ---------")
            try {
                dump()
            } catch {
                case e: Throwable =>
            }
            log.info("-------end dump ---------")
        }

        if (sos != null) {
            sos.closeReadChannel()
        }

        Thread.sleep(500)

        shutdown.set(true)

        if (regdishooks != null) {
            for (t <- regdishooks if t.isInstanceOf[Closable]) {
                t.asInstanceOf[Closable].close()
            }
        }
        for (t <- allBeans if t.isInstanceOf[Closable]) {
            t.asInstanceOf[Closable].close()
        }

        for (act <- allActors if act.isInstanceOf[BeforeClose]) {
            act.asInstanceOf[BeforeClose].beforeClose()
        }

        for (act <- allActors if act.isInstanceOf[Closable]) {
            act.asInstanceOf[Closable].close()
        }

        if (asyncLogActor.isInstanceOf[Closable]) {
            asyncLogActor.asInstanceOf[Closable].close()
        }

        for (t <- allReqFilters if t.isInstanceOf[Closable]) {
            t.asInstanceOf[Closable].close()
        }

        for (t <- allResFilters if t.isInstanceOf[Closable]) {
            t.asInstanceOf[Closable].close()
        }

        if (qte != null) {
            qte.close()
        }

        if (sos != null) {
            sos.close()
            sos = null
        }

        callGlobalClose()

        log.info("router stopped")
    }

    def getConfig(name: String, defaultValue: String = ""): String = {
        parameters.getOrElse(name, defaultValue)
    }

    def getRawConfig(name: String, defaultValue: String = ""): String = {
        cfgParameters.getOrElse("@" + name, defaultValue)
    }

    def readPluginsByType(ty: String): HashMapStringString = {

        val actorCfgMap = new HashMapStringString

        val plugUrls = getClass.getClassLoader.getResources(pluginConfFile)
        while (plugUrls.hasMoreElements) {

            val url = plugUrls.nextElement
            val lines = Source.fromURL(url).getLines.toList

            for (line <- lines if line != "" if !line.startsWith("#")) {
                val ss = line.split(",")
                if (ss(0) == ty) {
                    if (ss.size >= 3)
                        actorCfgMap.put(ss(1), ss(2))
                    else
                        actorCfgMap.put(ss(1), "")
                }
            }
        }

        actorCfgMap
    }

    def loadParameterFile(): HashMapStringString = {

        val pmap = HashMapStringString()
        val configfile = Router.configXml
        if (new File(rootDir + "/" + configfile).exists()) {
            val in = new InputStreamReader(new FileInputStream(rootDir + "/" + configfile), "UTF-8")
            val pxml = XML.load(in)
            in.close()

            loadParameter(pxml, pmap, "assign")
            loadParameter(pxml, pmap, "Assign")
        }
        val pfile = "config_parameter/" + Router.parameterXml
        if (new File(rootDir + "/" + pfile).exists()) {
            val in = new InputStreamReader(new FileInputStream(rootDir + "/" + pfile), "UTF-8")
            val pxml = XML.load(in)
            in.close()

            loadParameter(pxml, pmap, "assign")
            loadParameter(pxml, pmap, "Assign")
        }
        val pluginMap = readPluginsByType("inithook")
        if (pluginMap.size > 0) {

            val in = new InputStreamReader(new FileInputStream(rootDir + "/" + configfile), "UTF-8")
            val tXml = XML.load(in)
            in.close()

            for ((clsName, typeNode) <- pluginMap) {

                val nodeList = (tXml \ typeNode).toList

                if (nodeList.size > 0) {
                    try {
                        val hook = Class.forName(clsName).getConstructors()(0).newInstance(this, nodeList(0)).asInstanceOf[InitHook]
                        hook.loadParameter(pmap)
                    } catch {
                        case e: Throwable =>
                            log.error("plugin {} cannot be loaded", clsName)
                            throw e
                    }
                }

            }
        }

        pmap
    }

    def loadParameter(pxml: Elem, pmap: HashMapStringString, nodeName: String) {
        val parametersNode = (pxml \ nodeName)
        for (node <- parametersNode) {
            val str = node.text.toString
            val p = str.indexOf("=")
            if (p > 0) {
                val key = str.substring(0, p).trim()
                //val value = Utility.escape( str.substring(p+1).trim() ) // 不再向下兼容，特殊字符可通过cdata避免转义
                val value = str.substring(p + 1).trim()

                pmap.put(key, value)
            }
        }
    }

    def prepareConfigFile(): String = {

        val lines = FileUtils.readLines(new File(rootDir + "/" + Router.configXml), "UTF-8").asInstanceOf[java.util.List[String]]

        val pmap = loadParameterFile()
        cfgParameters = pmap

        if (pmap.size > 0) {

            for (i <- 0 until lines.size) {
                val s = lines.get(i)
                if (s.indexOf("@") >= 0) {
                    val ns = replaceParameter(s, pmap)
                    lines.set(i, ns)
                }
            }

        }

        var buff = new StringBuilder()
        for (i <- 0 until lines.size) {
            buff.append(lines.get(i)).append("\n")
        }
        buff.toString()
    }

    def replaceParameter(s: String, pmap: HashMapStringString): String = {

        var matchlist = pReg.findAllMatchIn(s).toBuffer
        if (matchlist.size == 0) {
            matchlist = pReg2.findAllMatchIn(s).toBuffer
        }
        if (matchlist.size == 0) {
            return s
        }

        var ns = s
        for (tag <- matchlist) {

            tag.matched match {
                case pRegName(name) =>
                    val v = pmap.getOrElse(name, null)
                    if (v != null) {
                        ns = ns.replace(name, v.toString)
                    }
                case _ =>
            }
        }
        ns
    }

    def updateXml(xml: String): String = {

        var outputXml = xml

        val pluginMap = readPluginsByType("regdishook")
        if (pluginMap.size > 0) {
            regdishooks = new ArrayBuffer[RegDisHook]()
            for ((clsName, typeNode) <- pluginMap) {

                val in = new StringReader(outputXml)
                val tXml = XML.load(in)
                in.close()

                val nodeList = (tXml \ typeNode).toList

                if (nodeList.size > 0) {
                    try {
                        val hook = Class.forName(clsName).getConstructors()(0).newInstance(this, nodeList(0)).asInstanceOf[RegDisHook]
                        regdishooks += hook
                        outputXml = hook.updateXml(outputXml)
                    } catch {
                        case e: Throwable =>
                            log.error("plugin {} cannot be loaded", clsName)
                            throw e
                    }
                }

            }
        }

        outputXml
    }

    def initInternal() {

        val str1 = prepareConfigFile()
        val str2 = updateXml(str1)
//println("----------------------------- final xml="+str2)        
        val in = new StringReader(str2)
        cfgXml = XML.load(in)
        in.close()

        codecs = new TlvCodecs(avenueConfDir)

        val parametersNode = (cfgXml \ "Parameter")
        for (p <- parametersNode) {
            val key = (p \ "@name").toString
            val value = p.text.toString
            parameters.put(key, value)
        }

        var s = getConfig("globalCls", "")
        if (s != "") globalCls = s

        // start sos
        val port = (cfgXml \ "SapPort").text.toInt
        if (port > 0 && startSos)
            sos = new Sos(this, port)
        else {
            sos = new DummySos()
            log.info("SapPort is 0, use dummy sos")
        }

        Router.main = this
        Flow.router = this

        callGlobalInit()

        getConfig("serviceIdsAllowed", "0").split(",").map(_.toInt).foreach(a => serviceIdsAllowed.add(a))
        getConfig("serviceIdsNotAllowed", "0").split(",").map(_.toInt).foreach(a => serviceIdsNotAllowed.add(a))
        getConfig("reverseServiceIds", "0").split(",").map(_.toInt).foreach(a => reverseServiceIds.add(a))

        var mustReachEnabledStr = getConfig("mustReach.enabled", "true").toLowerCase
        mustReachEnabled = mustReachEnabledStr == "1" || mustReachEnabledStr == "true" || mustReachEnabledStr == "yes" || mustReachEnabledStr == "t" || mustReachEnabledStr == "y"

        val actorCfgMap = readPluginsByType("actor")

        val notFlowServiceIds = HashSet[Int]()
        for ((clsName, typeNode) <- actorCfgMap) {
            val list = (cfgXml \ typeNode \ "ServiceId").map(_.text).flatMap(_.split(",")).map(_.toInt)
            list.foreach(a => notFlowServiceIds.add(a))
        }
        reverseServiceIds.foreach(a => notFlowServiceIds.add(a))

        val flowServiceIds = codecs.allServiceIds().filter(!notFlowServiceIds.contains(_))

        asyncLogActor = new AsyncLogActor(this)

        val flowActor = new FlowActor(this)
        for (serviceId <- flowServiceIds) {
            actorMap.put(serviceId.toString, flowActor)
        }

        val syncedFlowActors = new ArrayBuffer[Actor]()

        if (true) {
            val nodeList = cfgXml \ "SyncedFlowCfg"
            for (cfgNode <- nodeList) {

                var threadNum = 0
                val s = (cfgNode \ "@threadNum").text
                if (s != "") threadNum = s.toInt

                val actor = new FlowActor(this, threadNum)

                val serviceIds = (cfgNode \ "ServiceId").text
                val serviceIdsArray = serviceIds.split(",")
                for (n <- serviceIdsArray) {
                    actorMap.put(n, actor)
                }

                syncedFlowActors += actor
            }
        }

        val reqFilterCfgMap = readPluginsByType("requestfilter")

        for ((clsName, typeNode) <- reqFilterCfgMap) {
            val nodeList = cfgXml \ typeNode
            for (cfgNode <- nodeList) {

                try {
                    val reqFilter = Class.forName(clsName).getConstructors()(0).newInstance(this, cfgNode).asInstanceOf[RequestFilter]
                    allReqFilters += reqFilter
                } catch {
                    case e: Throwable =>
                        log.error("plugin {} cannot be loaded", clsName)
                        throw e
                }
            }
        }

        val resFilterCfgMap = readPluginsByType("responsefilter")

        for ((clsName, typeNode) <- resFilterCfgMap) {
            val nodeList = cfgXml \ typeNode
            for (cfgNode <- nodeList) {

                try {
                    val resFilter = Class.forName(clsName).getConstructors()(0).newInstance(this, cfgNode).asInstanceOf[ResponseFilter]
                    allResFilters += resFilter
                } catch {
                    case e: Throwable =>
                        log.error("plugin {} cannot be loaded", clsName)
                        throw e
                }
            }
        }

        // start must reach actor
        mustReachActor = new MustReachActor(this, cfgXml) // actors not ready yet

        // start actor by config sequence

        val typeNodes = new HashMap[String, String]()
        for ((clsName, typeNode) <- actorCfgMap) {
            typeNodes.put(typeNode, clsName)
        }

        val pluginActors = new ArrayBuffer[Actor]()
        for (cfgNode <- cfgXml.child if typeNodes.contains(cfgNode.label)) {

            val typeNode = cfgNode.label
            val clsName = typeNodes.getOrElse(typeNode, null)

            try {
                val actor = Class.forName(clsName).getConstructors()(0).newInstance(this, cfgNode).asInstanceOf[Actor]

                val serviceIds = (cfgNode \ "ServiceId").text
                val serviceIdsArray = serviceIds.split(",")
                for (n <- serviceIdsArray) {
                    actorMap.put(n, actor)
                }

                pluginActors += actor
            } catch {
                case e: Throwable =>
                    log.error("plugin {} cannot be loaded", clsName)
                    throw e
            }
        }

        syncedFlowActors.foreach(allActors += _)
        allActors += flowActor
        pluginActors.foreach(allActors += _)
        allActors += mustReachActor

        // config sos actor
        for (serviceId <- reverseServiceIds) {
            actorMap.put(serviceId.toString, sos)
        }

        for (act <- allActors if act.isInstanceOf[AfterInit]) {
            act.asInstanceOf[AfterInit].afterInit()
        }

        val beanCfgMap = readPluginsByType("bean")

        for ((clsName, typeNode) <- beanCfgMap) {
            val nodeList = cfgXml \ typeNode
            for (cfgNode <- nodeList) {

                try {
                    val bean = Class.forName(clsName).getConstructors()(0).newInstance(this, cfgNode).asInstanceOf[Bean]
                    allBeans += bean
                } catch {
                    case e: Throwable =>
                        log.error("bean {} cannot be loaded", clsName)
                        throw e
                }
            }
        }

        // wait for all init beans started
        for (bean <- allBeans) {
            if (bean.isInstanceOf[InitBean]) {
                val b = bean.asInstanceOf[InitBean]
                while (!b.isInited)
                    Thread.sleep(1000)
            }
        }

        log.info("router started")
    }

    def refresh() {
        for (t <- allActors if t.isInstanceOf[Refreshable]) {
            t.asInstanceOf[Refreshable].refresh()
        }
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()
        for (t <- allActors if t.isInstanceOf[SelfCheckLike]) {
            val d = t.asInstanceOf[SelfCheckLike].selfcheck()
            if (d != null)
                buff ++= d
        }
        for (t <- allBeans if t.isInstanceOf[SelfCheckLike]) {
            val d = t.asInstanceOf[SelfCheckLike].selfcheck()
            if (d != null)
                buff ++= d
        }

        buff
    }

    def dump() {

        sos.dump()
        asyncLogActor.asInstanceOf[Dumpable].dump()
        qte.dump()

        for (t <- allActors if t.isInstanceOf[Dumpable]) {
            t.asInstanceOf[Dumpable].dump()
        }
        for (t <- allBeans if t.isInstanceOf[Dumpable]) {
            t.asInstanceOf[Dumpable].dump()
        }

    }

    def send(rawReq: RawRequest) {

        val data = rawReq.data
        val connId = rawReq.connId
        val receivedTime = rawReq.receivedTime

        val actor = findActor(data.serviceId, data.msgId)

        if (actor == null) {
            return
        }

        if (actor.isInstanceOf[RawRequestActor]) {

            if (rawReq.persistId == 0)
                persist(rawReq)

            if (rawReq.persistId != 0) {
                rawReq.sender.receive(new RawRequestAckInfo(rawReq))
            }

            actor.receive(rawReq)
            return
        }

        if (!actor.eq(mockActor)) {
            if (!actor.isInstanceOf[FlowActor]) {
                if (!serviceIdsAllowed.contains(data.serviceId))
                    return
            } else {
                if (serviceIdsNotAllowed.contains(data.serviceId))
                    return
            }
        }

        if (data.msgId >= 1000) {
            val s = getConfig("disabledMsgIdFrom_" + data.serviceId)
            if (s != "") {
                if (data.msgId >= s.toInt) {
                    val remoteIp = rawReq.remoteIp
                    if (remoteIp != "127.0.0.1")
                        return
                }
            }
        }

        try {

            val xhead = TlvCodec4Xhead.decode(data.serviceId, data.xhead)

            val tlvCodec = findTlvCodec(data.serviceId)
            if (tlvCodec == null) {
                log.error("tlv codec not found,serviceid={}", data.serviceId)
                val info = genRawReqResInfo(rawReq, ResultCodes.SERVICE_NOT_FOUND)
                rawReq.sender.receive(info)
                asyncLogActor.receive(info)
                persistCommit(rawReq, ResultCodes.SERVICE_NOT_FOUND)
                return ;
            }

            val (body, ec) = tlvCodec.decodeRequest(data.msgId, data.body, data.encoding)
            if (ec != 0) {
                log.error("decode request error, serviceId=" + data.serviceId + ", msgId=" + data.msgId)
                val info = genRawReqResInfo(rawReq, ec)
                rawReq.sender.receive(info)
                asyncLogActor.receive(info)
                persistCommit(rawReq, ec)
                return
            }

            val req = new Request(
                rawReq.requestId,
                connId,
                data.sequence,
                data.encoding,
                data.serviceId,
                data.msgId,
                xhead,
                body,
                rawReq.sender)

            req.receivedTime = rawReq.receivedTime
            req.version = rawReq.data.version

            if (rawReq.persistId == 0)
                persist(rawReq)

            req.persistId = rawReq.persistId

            if (rawReq.persistId != 0) {
                rawReq.sender.receive(new RawRequestAckInfo(rawReq))
            }

            var i = 0
            while (i < allReqFilters.size) {
                allReqFilters(i).filter(req)
                i += 1
            }

            actor.receive(req)

            // allow local cache visited from outside directly
            if (actor.isInstanceOf[SyncedActor]) {
                val sc = actor.asInstanceOf[SyncedActor]
                val res = sc.get(req.requestId)
                reply(new RequestResponseInfo(req, res))

                asyncLogActor.receive(new RequestResponseInfo(req, res))
            }

        } catch {
            case e: Throwable =>
                log.error("process raw request error", e)
                val info = genRawReqResInfo(rawReq, ResultCodes.SERVICE_INTERNALERROR)
                rawReq.sender.receive(info)
                asyncLogActor.receive(info)
                persistCommit(rawReq, ResultCodes.SERVICE_INTERNALERROR)
        }
    }

    def reply(info: RawRequestResponseInfo): Unit = {

        persistCommit(info.rawReq, info.rawRes.data.code)

        if (info.rawReq.connId == Router.DO_NOT_REPLY) {
            asyncLogActor.receive(info)
            return
        }

        info.rawReq.sender.receive(info)

        asyncLogActor.receive(info)
    }

    def send(req: Request): InvokeResult = {

        val actor = findActor(req.serviceId, req.msgId)
        if (actor != null) {

            if (req.persistId == 0)
                persist(req)

            var i = 0
            while (i < allReqFilters.size) {
                allReqFilters(i).filter(req)
                i += 1
            }

            actor.receive(req)

            if (actor.isInstanceOf[SyncedActor]) {

                val sc = actor.asInstanceOf[SyncedActor]
                val res = sc.get(req.requestId)

                asyncLogActor.receive(new RequestResponseInfo(req, res))

                persistCommit(req, res.code)

                val invret = new InvokeResult(res.requestId, res.code, res.body)
                invret

            } else {
                null
            }

        } else {
            log.error("serviceId not found in router.send")
            InvokeResult.failed(req.requestId)
        }

    }

    // send result, called by actor
    def reply(info: RequestResponseInfo): Unit = {

        val res = info.res

        persistCommit(info.req, res.code)

        var i = 0
        while (i < allResFilters.size) {
            allResFilters(i).filter(res, info.req)
            i += 1
        }

        if (res.connId == Router.DO_NOT_REPLY) {
            asyncLogActor.receive(info)
            return
        }

        val tlvCodec = findTlvCodec(res.serviceId)
        if (tlvCodec == null) {
            asyncLogActor.receive(info)
            return ;
        }

        /*
        if( log.isDebugEnabled() ) {
            log.debug("router reply called, res="+res)
        }
         */

        if (res.sender != null && res.sender.isInstanceOf[RawRequestActor]) {

            val (body, ec) = tlvCodec.encodeResponse(res.msgId, res.body, res.encoding)
            var errorCode = res.code
            if (errorCode == 0 && ec != 0) {
                log.error("encode response error, serviceId=" + res.serviceId + ", msgId=" + res.msgId)
                errorCode = ec
                info.res.code = ec
            }

            var version = info.req.version
            if( version == 0 ) version = codecs.version(res.serviceId)
            val data = new AvenueData(
                AvenueCodec.TYPE_RESPONSE, 
                version,
                res.serviceId, res.msgId, 
                res.sequence,
                0, res.encoding,
                errorCode,
                EMPTY_BUFFER,
                body)

            res.sender.receive(new RawResponse(data, res.connId))
        }

        if (res.sender != null && !res.sender.isInstanceOf[RawRequestActor]) { // from flow,localqueue,...

            val (newbody, ec) = tlvCodec.filterResponseInJvm(res.msgId, res.body)
            var errorCode = res.code
            if (errorCode == 0 && ec != 0) {
                log.error("encode responseinjvm error, serviceId=" + res.serviceId + ", msgId=" + res.msgId)
                errorCode = ec
                info.res.code = ec
            }
            info.res.body = newbody
            res.sender.receive(new InvokeResult(res.requestId, errorCode, newbody))

        }

        asyncLogActor.receive(info)
    }

    def findActor(serviceId: Int, msgId: Int): Actor = {
        if (mockMode) {
            val buff = mocks.getOrElse(serviceId + ":" + msgId, null)
            if (buff != null) {
                return mockActor
            }
        }
        val a = actorMap.getOrElse(serviceId.toString, null)
        if (a != null && !a.isInstanceOf[FlowActor]) {
            return a
        }
        val b = actorMap.getOrElse(serviceId.toString + "." + msgId.toString, null)
        if (b != null) {
            return b
        }
        val c = actorMap.getOrElse(serviceId.toString + ".*", null)
        if (c != null) {
            return c
        }
        return a
    }

    def encodeRequest(serviceId: Int, msgId: Int, body: HashMapStringAny): Tuple2[HashMapStringAny, Int] = {

        val tlvCodec = findTlvCodec(serviceId)

        if (tlvCodec != null) {
            // 对RawRequestActor数据将通过网络发出，不需要编码；对其它类型的actor, 数据编码后在进程内直接使用，没有解码过程，所以需要直接进行编码
            val a = actorMap.getOrElse(serviceId.toString, null)
            val needEncode = if (a != null && a.isInstanceOf[RawRequestActor]) false else true
            val tp = tlvCodec.filterRequestInJvm(msgId, body, needEncode)
            if (tp._2 != 0) {
                log.error("encode request in jvm error, serviceId=" + serviceId + ", msgId=" + msgId)
            }
            return tp
        } else {
            (body, 0)
        }
    }

    def encodeResponse(serviceId: Int, msgId: Int, code: Int, body: HashMapStringAny): Tuple2[HashMapStringAny, Int] = {

        val tlvCodec = findTlvCodec(serviceId)

        if (tlvCodec != null) {
            val tp = tlvCodec.filterResponseInJvm(msgId, body)
            if (tp._2 != 0) {
                log.error("encode response in jvm error, serviceId=" + serviceId + ", msgId=" + msgId)
            }
            return tp
        } else {
            (body, 0)
        }
    }

    def findTlvCodec(serviceId: Int): TlvCodec = {
        codecs.findTlvCodec(serviceId)
    }

    def serviceNameToId(service: String): Tuple2[Int, Int] = {
        codecs.serviceNameToId(service)
    }

    def serviceIdToName(serviceId: Int, msgId: Int): Tuple2[String, String] = {
        codecs.serviceIdToName(serviceId, msgId)
    }

    // timeout management

    def newTimer(timeoutId: String, t: Int): QuickTimer = {
        qte.newTimer(t, timeoutId)
    }

    def timeoutId(tp: Int, serviceId: Int, msgId: Int, requestId: String): String = {
        tp + "#" + serviceId + "#" + msgId + "#" + requestId
    }

    def splitTimeoutId(timeoutId: String): Tuple4[Int, Int, Int, String] = {
        val l = timeoutId.split("#")
        val tp = l(0).toInt
        val serviceId = l(1).toInt
        val msgId = l(2).toInt
        val requestId = l(3)
        (tp, serviceId, msgId, requestId)
    }

    def timeout_function(any: Any) {
        val timeoutId = any.asInstanceOf[String]
        val (tp, serviceId, msgId, requestId) = splitTimeoutId(timeoutId)
        val actor = findActor(serviceId, msgId)
        if (actor != null) {
            tp match {
                case FlowTimoutType.TYPE_FLOW =>
                    actor.receive(FlowTimeout(requestId))
                case FlowTimoutType.TYPE_CALL =>
                    actor.receive(FlowCallTimeout(requestId))
                case _ =>
            }
        }
    }

    // "must reach" management

    def persist(rawReq: RawRequest): Unit = {
        if (!mustReachEnabled) return
        mustReachActor.receive(rawReq)
    }
    def persist(req: Request): Unit = {
        if (!mustReachEnabled) return
        mustReachActor.receive(req)
    }

    def persistCommit(req: Request, code: Int): Unit = {
        if (req.persistId == 0) return
        mustReachActor.receive(MustReachReqCommitInfo(req, code))
    }
    def persistCommit(rawReq: RawRequest, code: Int): Unit = {
        if (rawReq.persistId == 0) return
        mustReachActor.receive(MustReachRawReqCommitInfo(rawReq, code))
    }

    def receiveAck(info: RawRequestAckInfo): Unit = {
        persistCommit(info.rawReq, 0)
    }
    def receiveAck(info: RequestAckInfo): Unit = {
        persistCommit(info.req, 0)
    }

    def genRawReqResInfo(req: RawRequest, code: Int): RawRequestResponseInfo = {
        val data = new AvenueData(
            AvenueCodec.TYPE_RESPONSE, 
            req.data.version,
            req.data.serviceId, req.data.msgId, 
			req.data.sequence,
            0, req.data.encoding,
            code,
            EMPTY_BUFFER,
            EMPTY_BUFFER)

        val res = new RawResponse(data, req.connId)
        val info = new RawRequestResponseInfo(req, res)
        info
    }

}

