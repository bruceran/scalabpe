package scalabpe.plugin

import scala.collection.mutable.HashMap
import scala.xml.Node

import scalabpe.core.DummyActor
import scalabpe.core.HashMapStringAny
import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.Response
import scalabpe.core.ResponseFilter
import scalabpe.core.Router

class ErrorCodeDefine(val resultCodeName: String, val resultMsgName: String);

class ErrorDescResponseFilter(val router: Router, val cfgNode: Node) extends ResponseFilter with Logging {

    val cfgs = new HashMap[Int, ErrorCodeDefine]()
    var localCacheServiceId = 0
    val dummyActor = new DummyActor()

    init

    def init() {

        var s = (cfgNode \ "@localCacheServiceId").toString
        if (s != "")
            localCacheServiceId = s.toInt

        val serviceNodes = (cfgNode \ "Service")
        for (p <- serviceNodes) {

            val serviceId = (p \ "@serviceId").toString.toInt
            val resultCodeName = (p \ "@resultCodeField").toString
            val resultMsgName = (p \ "@resultMsgField").toString

            cfgs.put(serviceId, new ErrorCodeDefine(resultCodeName, resultMsgName))

            // log.info("serviceId=%d,resultCodeName=%s,resultMsgName=%s".format(serviceId,resultCodeName,resultMsgName))
        }

        log.info("errorcode response filter created")
    }

    def filter(res: Response, req: Request): Unit = {

        // log.info("error response filter called, res={}",res.toString)

        val rd = cfgs.getOrElse(res.serviceId, null)
        if (rd == null) return

        if (rd.resultCodeName != "") {
            if (res.body.getOrElse(rd.resultCodeName, null) == null) {
                res.body.put(rd.resultCodeName, res.code)
            }
        }

        if (res.code == 0) return
        if (rd.resultMsgName == "") return
        if (res.body.getOrElse(rd.resultMsgName, null) != null) return

        val body = new HashMapStringAny()
        body.put("resultCode", res.code)

        val req = new Request(
            res.requestId + ":$",
            Router.DO_NOT_REPLY,
            res.sequence,
            res.encoding,
            localCacheServiceId,
            1,
            new HashMapStringAny(),
            body,
            dummyActor)

        val invokeResult = router.send(req)
        if (invokeResult == null) return

        val resultMsg = invokeResult.s("resultMsg", "")
        if (resultMsg != "")
            res.body.put(rd.resultMsgName, resultMsg)
    }

}

