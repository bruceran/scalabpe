package scalabpe.plugin

import scala.xml.Node

import scalabpe.core.Closable
import scalabpe.core.Logging
import scalabpe.core.RegDisHook
import scalabpe.core.Router

class RegDisHookSample(val router: Router, val cfgNode: Node) extends Logging with RegDisHook with Closable {

    log.info("regdis hook created")

    def updateXml(xml: String): String = {
        log.info("updateXml called")
        xml
    }

    def close(): Unit = {
        log.info("regdis hook closed")
    }
}
