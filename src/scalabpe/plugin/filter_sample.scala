package scalabpe.plugin

import scala.xml.Node

import scalabpe.core.Logging
import scalabpe.core.Request
import scalabpe.core.RequestFilter
import scalabpe.core.Response
import scalabpe.core.ResponseFilter
import scalabpe.core.Router

class SampleRequestFilter(val router: Router, val cfgNode: Node) extends Logging with RequestFilter {

    log.info("sample request filter created")

    def filter(req: Request): Unit = {
        log.info("sample request filter called, req={}", req.toString)
    }
}

class SampleResponseFilter(val router: Router, val cfgNode: Node) extends Logging with ResponseFilter {

    log.info("sample response filter created")

    def filter(res: Response, req: Request): Unit = {
        log.info("sample response filter called, res={}", res.toString)
    }

}
