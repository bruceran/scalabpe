package scalabpe.plugin.etcdreg

import java.io._
import java.util.concurrent._
import java.util.concurrent.locks._
import java.util.concurrent.atomic._
import java.util.{ TimerTask, Timer }
import java.nio.charset.Charset
import scala.collection.mutable.{ HashMap, ArrayBuffer, HashSet }
import scala.xml._
import org.apache.commons.io.FileUtils;
import org.jboss.netty.buffer._;
import org.jboss.netty.handler.codec.http._;

import scalabpe.core._
import scalabpe.plugin._

class EtcdRegPlugin(val router: Router, val cfgNode: Node)
        extends Logging with RegDisHook with Closable {

    var hosts = ""
    var basePath = "/v2/keys/services"
    var ttl = 90
    var interval = 30

    var configBasePath = "/v2/keys/configs"
    var enableConfigService = false
    var configPath = ""
    var globalConfigPath = ""

    var profile = "default"
    val timer = new Timer("etcdregtimer")
    var instanceId = ""
    var sosMap = HashMap[String, String]() // serviceId->discoverFor
    var addrsMap = HashMap[String, String]() // serviceId->addrs
    var regMap = HashMap[String, String]() // registerAs->ip:port
    var unRegOnCloseMap = HashMap[String, Boolean]() // registerAs->unregisterOnClose  flag
    var etcdClient: EtcdHttpClient = _
    var waitForRouterTask: TimerTask = _

    def uuid(): String = {
        return java.util.UUID.randomUUID().toString().replaceAll("-", "")
    }

    init

    def init() {
        profile = Router.profile
        var s = ""

        val instanceIdFile = new File(Router.dataDir + "/instance_id")
        if (instanceIdFile.exists) {
            s = FileUtils.readFileToString(instanceIdFile)
        }

        if (s != "")
            instanceId = s
        else {
            instanceId = uuid()
            FileUtils.writeStringToFile(instanceIdFile, instanceId)
        }

        s = (cfgNode \ "Hosts").text
        if (s == "")
            throw new Exception("ETCD hosts not configed")
        hosts = s

        s = (cfgNode \ "@basePath").text
        if (s != "") basePath = s

        s = (cfgNode \ "@ttl").text
        if (s != "") ttl = s.toInt

        s = (cfgNode \ "@interval").text
        if (s != "") interval = s.toInt

        s = (cfgNode \ "@enableConfigService").text
        if (s != "") enableConfigService = TypeSafe.isTrue(s)

        s = (cfgNode \ "@configPath").text
        if (s != "") configPath = s
        if( configPath == "" )
            configPath = System.getProperty("application.name")

        s = (cfgNode \ "@globalConfigPath").text
        if (s != "") globalConfigPath = s

        etcdClient = new EtcdHttpClient(hosts, 15000, 1000)
    }

    def close(): Unit = {
        timer.cancel()
        doUnRegister()
        etcdClient.close()
        log.info("etcdreg plugin closed")
    }

    def saveAddrsToFile(serviceId: String, addrs: String) {
        val dir = Router.dataDir + File.separator + "etcd"
        if (!new File(dir).exists) new File(dir).mkdirs()
        val file = dir + File.separator + serviceId
        FileUtils.writeStringToFile(new File(file), addrs)
    }
    def loadAddrsFromFile(serviceId: String): Tuple2[Boolean, String] = {
        val dir = Router.dataDir + File.separator + "etcd"
        val file = dir + File.separator + serviceId
        if (!new File(file).exists) return (false, "")
        val addrs = FileUtils.readFileToString(new File(file))
        (true, addrs)
    }

    def updateXml(xml: String): String = {

        var newxml = xml
        if(enableConfigService) {
            newxml = updateFromConfigServer(newxml)
        }
        val in = new StringReader(newxml)
        val cfgXml = XML.load(in)
        in.close()

        var sos_nodelist = (cfgXml \ "SosList")
        for (sos_node <- sos_nodelist) {
            var s = (sos_node \ "@discoverFor").text
            if (s != "") {
                var serviceId = (sos_node \ "ServiceId").text.split(",")(0)
                sosMap.put(serviceId, s)
            }
        }
        log.info("etcd discovery: sosMap=" + sosMap.mkString(","))

        for ((serviceId, discoverFor) <- sosMap) {
            val (ok, addrs) = getEtcdAddrs(profile, discoverFor)
            if (ok) {
                addrsMap.put(serviceId, addrs)
                saveAddrsToFile(serviceId, addrs)
            } else {
                val (ok2, addrs2) = loadAddrsFromFile(serviceId)
                if (ok2) {
                    addrsMap.put(serviceId, addrs)
                    log.warn("cannot get addrs from etcd,use local cache")
                } else {
                    log.warn("cannot get addrs from etcd,use config in config.xml")
                }
            }
        }
        log.info("etcd discovery: addrsMap=" + addrsMap.mkString(","))

        var sapPort = (cfgXml \ "SapPort").text
        if (sapPort != "") {
            var registerAs = (cfgXml \ "Server" \ "@registerAs").text
            if (registerAs != "") {
                regMap.put(registerAs, getHost() + ":" + getTcpPort(sapPort))
                val s = (cfgXml \ "Server" \ "@unregisterOnClose").text
                unRegOnCloseMap.put(registerAs, TypeSafe.isTrue(s))
            }
        }
        var httpservers = (cfgXml \ "HttpServerCfg")
        for (httpserver_node <- httpservers) {
            var registerAs = (httpserver_node \ "@registerAs").text
            if (registerAs != "") {
                var port = (httpserver_node \ "@port").text
                regMap.put(registerAs, getHost() + ":" + getHttpPort(port))
                val s = (httpserver_node \ "@unregisterOnClose").text
                unRegOnCloseMap.put(registerAs, TypeSafe.isTrue(s))
            }
        }
        log.info("etcd registry plugin, regMap=" + regMap.mkString(","))

        waitForRouterTask = new TimerTask() {
            def run() {
                waitForRouter()
            }
        }

        timer.schedule(waitForRouterTask, 1 * 1000, 1 * 1000)

        log.info("etcdreg plugin inited")

        for ((serviceId, discoverFor) <- sosMap) {
            val addrs = addrsMap.getOrElse(serviceId, null)
            if (addrs != null)
                newxml = replaceXml(newxml, serviceId, discoverFor, addrs)
        }

        newxml
    }

    def replaceXml(xml: String, serviceId: String, discoverFor: String, new_addrs: String): String = {
        val tag1 = "discoverFor=\"%s\"".format(discoverFor)
        val p1 = xml.indexOf(tag1)
        if (p1 < 0) return xml

        val tag2 = "<ServiceId>%s".format(serviceId)
        val p2 = xml.indexOf(tag2, p1)
        if (p2 < 0) return xml

        val tag3 = "</ServiceId>"
        val p3 = xml.indexOf(tag3, p2)
        if (p3 < 0) return xml

        val tag4 = "</SosList>"
        val p4 = xml.indexOf(tag4, p3)
        if (p4 < 0) return xml

        val ss = new_addrs.split(",")
        val addrs = ArrayBufferString()
        for (s <- ss) {
            addrs += "<ServerAddr>%s</ServerAddr>".format(s)
        }

        val newxml = xml.substring(0, p3 + tag3.length) + "\n" + addrs.mkString("\n") + "\n" + xml.substring(p4)
        newxml
    }

    def reconfig(serviceId: String, addrs: String) {
        try {
            val socActor = Router.main.findActor(serviceId.toInt, 1).asInstanceOf[SocActor]
            if (socActor == null) return
            val socWrapper = socActor.socWrapper
            if (socWrapper == null) return
            val nc = socWrapper.nettyClient
            if (nc == null) return
            nc.reconfig(addrs)
        } catch {
            case e: Throwable =>
                log.error("cannot reconfig soc, e=" + e.getMessage)
        }
    }

    def waitForRouter() {
        if (Router.main == null) return
        if (!Router.main.started.get()) return
        waitForRouterTask.cancel()
        doRegister()
        timer.schedule(new TimerTask() {
            def run() {
                refresh()
            }
        }, interval * 1000, interval * 1000)
    }

    def refresh() {
        doRegister()
        doDiscover()
    }

    def doDiscover() {

        for ((serviceId, discoverFor) <- sosMap) {
            val (ok, addrs) = getEtcdAddrs(profile, discoverFor)
            if (ok) {
                saveAddrsToFile(serviceId, addrs)
                val oldAddrs = addrsMap.getOrElse(serviceId, null)
                if (oldAddrs == null || oldAddrs != addrs) {
                    addrsMap.put(serviceId, addrs)
                    reconfig(serviceId, addrs)
                }
            } else {
                log.error("cannot get addrs from etcd")
            }
        }
    }

    def doRegister() {
        for ((registerAs, addr) <- regMap) {
            val ok = registerToEtcd(profile, registerAs, addr)
            if (!ok) {
                log.error("cannot register service to etcd, name=" + registerAs)
            }
        }
    }

    def doUnRegister() {
        for ((registerAs, addr) <- regMap if unRegOnCloseMap.getOrElse(registerAs, false)) {
            val ok = unRegisterToEtcd(profile, registerAs, addr)
            if (!ok) {
                log.error("cannot unregister service to etcd, name=" + registerAs)
            }
        }
    }

    def getHost(): String = {
        IpUtils.localIp()
    }

    def getTcpPort(port: String): String = {
        val env_port = System.getenv("SCALABPE_TCP_PORT") // used in docker
        if (env_port != null && env_port != "") {
            return env_port
        }
        port
    }
    def getHttpPort(port: String): String = {
        val env_port = System.getenv("SCALABPE_HTTP_PORT") // used in docker
        if (env_port != null && env_port != "") {
            return env_port
        }
        port
    }

    def getEtcdAddrs(profile: String, discoverFor: String): Tuple2[Boolean, String] = {
        val path = basePath + "/" + profile + "/" + discoverFor
        val (errorCode, content) = etcdClient.sendWithReturn("get", path, "")
        if (errorCode != 0 || log.isDebugEnabled()) {
            log.info("etcd req: path=" + path + " res: errorCode=" + errorCode + ",content=" + content)
        }

        if (errorCode != 0) return (false, "")
        val m = JsonCodec.parseObjectNotNull(content)
        if (m.size == 0) return (false, "")
        if (m.ns("errorCode") != "" || m.ns("action") != "get") return (false, "")

        val nodelist = m.nm("node").nlm("nodes")
        val addrs = HashSet[String]()

        for (m <- nodelist if m.ns("value") != "") {
            addrs.add(m.ns("value"))
        }

        /*
        $curl http://192.168.28.25:2379/v2/keys/services/prd/userservice*
        {"action":"get","node":{"key":"/services/prd/userservice","dir":true,
            "nodes":[
                {"key":"/services/prd/userservice/111","value":"192.168.1.13:6111","modifiedIndex":279865,"createdIndex":279865},
                {"key":"/services/prd/userservice/112","value":"192.168.1.13:6112","modifiedIndex":279866,"createdIndex":279866}
            ],"modifiedIndex":279865,"createdIndex":279865}}
        */

        val sortedAddrs = addrs.toBuffer.sorted
        (true, sortedAddrs.mkString(","))
    }

    def registerToEtcd(profile: String, registerAs: String, addr: String): Boolean = {
        val path = basePath + "/" + profile + "/" + registerAs + "/" + instanceId + "?ttl=" + ttl
        val value = "value=%s".format(addr)
        val (errorCode, content) = etcdClient.sendWithReturn("put", path, value)

        if (errorCode != 0 || log.isDebugEnabled()) {
            log.info("etcd req: path=" + path + ",value=" + value + " res: errorCode=" + errorCode + ",content=" + content)
        }

        if (errorCode != 0) return false
        val m = JsonCodec.parseObjectNotNull(content)
        if (m.size == 0) return false
        if (m.ns("errorCode") != "" || m.ns("action") != "set") return false

        /*
        $curl http://192.168.28.25:2379/v2/keys/services/prd/userservice/113?ttl=5  -XPUT -d value=192.168.1.13:6113
        {"action":"set","node":{"key":"/services/prd/userservice/113","value":"192.168.1.13:6113","expiration":"2017-04-17T08:55:32.561426392Z","ttl":5,"modifiedIndex":279880,"createdIndex":279880}}

        $curl http://192.168.28.25:2379/v2/keys/services/prd/userservice  -XPUT -d value=192.168.1.13:6113
        {"errorCode":102,"message":"Not a file","cause":"/services/prd/userservice","index":279881}
         */

        true
    }

    def unRegisterToEtcd(profile: String, registerAs: String, addr: String): Boolean = {
        val path = basePath + "/" + profile + "/" + registerAs + "/" + instanceId
        val value = ""
        val (errorCode, content) = etcdClient.sendWithReturn("delete", path, value)

        if (errorCode != 0 || log.isDebugEnabled()) {
            log.info("etcd req: path=" + path + ",value=" + value + " res: errorCode=" + errorCode + ",content=" + content)
        }

        if (errorCode != 0) return false
        val m = JsonCodec.parseObjectNotNull(content)
        if (m.size == 0) return false
        if (m.ns("errorCode") != "" || m.ns("action") != "delete") return false

        /*
        $curl http://192.168.28.25:2379/v2/keys/services/prd/userservice/113  -XDELETE 

        */

        true
    }

    def updateFromConfigServer(xml: String): String = {
        val names = parseConfigNames(xml)
        if( names.size == 0 ) return xml

        val (ok,configsGlobal) = getEtcdConfig(profile,globalConfigPath)
        val (ok2,configsApp) = getEtcdConfig(profile,configPath)
        if( ok || ok2 ) {
            configsGlobal ++= configsApp
        } else {
            val (ok3,configsStr) = loadConfigFromFile()
            if(!ok3)
                throw new RuntimeException("config server parameter cannot be loaded")
            configsGlobal.clear()
            configsGlobal ++= JsonCodec.parseObjectNotNull(configsStr)
        }
        var newxml = xml
        val savedConfigs = HashMapStringAny()
        for( name <- names ) {
            val value = configsGlobal.ns(name)
            if( value == "" ) 
                throw new RuntimeException("config server parameter %s not found".format(name))
            newxml = newxml.replaceAll("@configservice:"+name,value)
            savedConfigs.put(name,value)
        }
        if( ok || ok2 ) {
            saveConfigToFile(JsonCodec.mkString(savedConfigs)) 
        }
        newxml
    }

    def parseConfigNames(xml:String):ArrayBufferString = {
        val buff = ArrayBufferString()

        val lines = xml.split("\n")

        val reg1 = """@configservice:[0-9a-zA-Z_]+[^0-9a-zA-Z_]|@configservice:[0-9a-zA-Z_]+$""".r
        val reg2 = """@configservice:([0-9a-zA-Z_]+).*$""".r

        for (i <- 0 until lines.size) {
            val s = lines(i)
            val matchlist = reg1.findAllMatchIn(s)
            if (matchlist != null) {

                val tags = matchlist.toList
                for (tag <- tags) {

                    tag.matched match {
                        case reg2(key) =>
                            buff += key
                        case _ =>
                    }
                }
            }
        }

        buff
    }

    def saveConfigToFile(configs:String) {
        val dir = Router.dataDir + File.separator + "etcd"
        if (!new File(dir).exists) new File(dir).mkdirs()
        val file = dir + File.separator + "configs"
        FileUtils.writeStringToFile(new File(file), configs)
    }
    def loadConfigFromFile(): Tuple2[Boolean, String] = {
        val dir = Router.dataDir + File.separator + "etcd"
        val file = dir + File.separator + "configs"
        if (!new File(file).exists) return (false, "")
        val configs = FileUtils.readFileToString(new File(file))
        (true, configs)
    }

    def getEtcdConfig(profile: String, appName: String): Tuple2[Boolean, HashMapStringAny] = {
        val path = configBasePath + "/" + profile + "/" + appName
        val (errorCode, content) = etcdClient.sendWithReturn("get", path, "")
        if (errorCode != 0 || log.isDebugEnabled()) {
            log.info("etcd req: path=" + path + " res: errorCode=" + errorCode + ",content=" + content)
        }

        if (errorCode != 0) return (false, HashMapStringAny())
        val m = JsonCodec.parseObjectNotNull(content)
        if (m.size == 0) return (false, HashMapStringAny())
        if (m.ns("errorCode") != "" || m.ns("action") != "get") return (false, HashMapStringAny())

        val nodelist = m.nm("node").nlm("nodes")
        val configs = HashMapStringAny()

        for (m <- nodelist if m.ns("key") != "" && m.ns("value") != "") {
            val key = m.ns("key") 
            val p = key.lastIndexOf("/")
            configs.put(key.substring(p+1),m.ns("value"))
        }

        /*

            [root@rpf02 app]# curl http://10.246.84.91:2379/v2/keys/configs/dev/scalabpedev
            {"action":"get","node":{"key":"/configs/dev/scalabpedev","dir":true,"nodes":[{"key":"/configs/dev/scalabpedev/x","value":"thisisx","modifiedIndex":303184,"createdIndex":303184}],"modifiedIndex":303184,"createdIndex":303184}}
            [root@rpf02 app]# curl http://10.246.84.91:2379/v2/keys/configs/dev/scalabpedev/y  -XPUT -d value=thisisy
            {"action":"set","node":{"key":"/configs/dev/scalabpedev/y","value":"thisisy","modifiedIndex":303187,"createdIndex":303187}}
            [root@rpf02 app]# curl http://10.246.84.91:2379/v2/keys/configs/dev/scalabpedev
            {"action":"get","node":{"key":"/configs/dev/scalabpedev","dir":true,"nodes":[{"key":"/configs/dev/scalabpedev/x","value":"thisisx","modifiedIndex":303184,"createdIndex":303184},{"key":"/configs/dev/scalabpedev/y","value":"thisisy","modifiedIndex":303187,"createdIndex":303187}],"modifiedIndex":303184,"createdIndex":303184}}
            [root@rpf02 app]# curl http://10.246.84.91:2379/v2/keys/configs/dev/scalabpedev/y  -XDELETE
            {"action":"delete","node":{"key":"/configs/dev/scalabpedev/y","modifiedIndex":303188,"createdIndex":303187},"prevNode":{"key":"/configs/dev/scalabpedev/y","value":"thisisy","modifiedIndex":303187,"createdIndex":303187}}
            [root@rpf02 app]# curl http://10.246.84.91:2379/v2/keys/configs/dev/scalabpedev/x  -XDELETE
            {"action":"delete","node":{"key":"/configs/dev/scalabpedev/x","modifiedIndex":303189,"createdIndex":303184},"prevNode":{"key":"/configs/dev/scalabpedev/x","value":"thisisx","modifiedIndex":303184,"createdIndex":303184}}
            [root@rpf02 app]# curl http://10.246.84.91:2379/v2/keys/configs/dev/scalabpedev?dir=true  -XDELETE
            {"action":"delete","node":{"key":"/configs/dev/scalabpedev","dir":true,"modifiedIndex":303190,"createdIndex":303184},"prevNode":{"key":"/configs/dev/scalabpedev","dir":true,"modifiedIndex":303184,"createdIndex":303184}}

        */

        (true, configs)
    }
}

class EtcdHttpClient(
        val hosts: String,
        val connectTimeout: Int = 15000,
        val timerInterval: Int = 1000) extends HttpClient4Netty with Logging {

    var nettyHttpClient: NettyHttpClient = _
    val generator = new AtomicInteger(1)
    val dataMap = new ConcurrentHashMap[Int, CacheData]()
    val localIp = IpUtils.localIp()
    val timeout = 10000
    var hostsArray = hosts.split(",")
    var hostsIdx = new AtomicInteger(0)

    init

    def init() {
        nettyHttpClient = new NettyHttpClient(this, connectTimeout, timerInterval)
        log.info("EtcdHttpClient started")
    }

    def close() {
        nettyHttpClient.close()
        log.info("EtcdHttpClient stopped")
    }

    def sendWithReturn(method: String, path: String, body: String): Tuple2[Int, String] = {
        val lock = new ReentrantLock();
        val cond = lock.newCondition();
        val r = new AtomicReference[Tuple2[Int, String]]();
        val callback = (res: Tuple2[Int, String]) => {
            lock.lock();
            try {
                r.set(res);
                cond.signal();
            } finally {
                lock.unlock();
            }
        }

        lock.lock();
        try {
            var idx = hostsIdx.get()
            if (idx > hostsArray.length) idx = 0
            val host = hostsArray(idx)
            send(method, host, path, body, callback)
            try {
                val ok = cond.await(timeout, TimeUnit.MILLISECONDS);
                if (ok) {
                    return r.get();
                }
                skipIdx(idx)
                return (ResultCodes.SERVICE_TIMEOUT, "")
            } catch {
                case e: InterruptedException =>
                    skipIdx(idx)
                    return (ResultCodes.SOC_NETWORKERROR, "")
            }
        } finally {
            lock.unlock();
        }

    }

    def skipIdx(idx: Int) {
        val nextIdx = if (idx + 1 >= hostsArray.length) 0 else idx + 1
        hostsIdx.compareAndSet(idx, nextIdx)
    }

    def send(method: String, host: String, path: String, body: String, callback: (Tuple2[Int, String]) => Unit): Unit = {
        var httpReq = generateRequest(host, path, method, body)
        val sequence = generateSequence()
        dataMap.put(sequence, new CacheData(callback))

        nettyHttpClient.send(sequence, false, host, httpReq, timeout)
    }

    def receive(sequence: Int, httpRes: HttpResponse): Unit = {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val tpl = parseResult(httpRes)
        saved.callback(tpl)
    }

    def networkError(sequence: Int) {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val tpl = (ResultCodes.SOC_NETWORKERROR, "")
        saved.callback(tpl)
    }

    def timeoutError(sequence: Int) {
        val saved = dataMap.remove(sequence)
        if (saved == null) return
        val tpl = (ResultCodes.SOC_TIMEOUT, "")
        saved.callback(tpl)
    }

    def generateSequence(): Int = {
        generator.getAndIncrement()
    }

    def generateRequest(host: String, path: String, method: String, body: String): HttpRequest = {

        val m = method.toLowerCase match {
            case "get"    => HttpMethod.GET
            case "put"    => HttpMethod.PUT
            case "post"   => HttpMethod.POST
            case "delete" => HttpMethod.DELETE
            case _        => HttpMethod.GET
        }
        val httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, m, path)

        val buffer = new DynamicChannelBuffer(512);
        buffer.writeBytes(body.getBytes("UTF-8"))

        if (log.isDebugEnabled()) {
            log.debug("method=" + method + ", path=" + path + ", content=" + body)
        }

        httpReq.setContent(buffer);
        httpReq.setHeader("Host", host) // the host include port already
        if (method.toLowerCase != "get") {
            httpReq.setHeader("Content-Type", "application/x-www-form-urlencoded")
            httpReq.setHeader("Content-Length", httpReq.getContent().writerIndex())
        }
        httpReq.setHeader("User-Agent", "scalabpe etcd client")
        httpReq.setHeader("Connection", "close")

        httpReq
    }

    def parseResult(httpRes: HttpResponse): Tuple2[Int, String] = {

        val status = httpRes.getStatus
        if (status.getCode() != 200 && status.getCode() != 201) {

            if (log.isDebugEnabled()) {
                log.debug("status code={}", status.getCode())
            }

            return (ResultCodes.SOC_NETWORKERROR, "")
        }

        val contentTypeStr = httpRes.getHeader("Content-Type")
        val content = httpRes.getContent()
        val contentStr = content.toString(Charset.forName("UTF-8"))

        if (log.isDebugEnabled()) {
            log.debug("contentType={},contentStr={}", contentTypeStr, contentStr)
        }

        (0, contentStr)
    }

    class CacheData(val callback: (Tuple2[Int, String]) => Unit)

}


