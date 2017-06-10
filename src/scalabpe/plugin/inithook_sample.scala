package scalabpe.plugin

import scala.xml.Node

import scalabpe.core.HashMapStringString
import scalabpe.core.InitHook
import scalabpe.core.Logging
import scalabpe.core.Router

class InitHookSample(val router: Router, val cfgNode: Node) extends Logging with InitHook {

    log.info("init hook created")

    def loadParameter(pmap: HashMapStringString): Unit = {
        log.info("loadParameter called")
    }
}
