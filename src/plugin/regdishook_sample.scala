package scalabpe.plugin

import scala.xml._

import scalabpe.core._

class RegDisHookSample(val router:Router,val cfgNode: Node) extends Logging with RegDisHook with Closable {

    log.info("regdis hook created")

    def updateXml(xml:String):String = {
        log.info("updateXml called")
        xml
    }

    def close():Unit = {
        log.info("regdis hook closed")
    }
}
