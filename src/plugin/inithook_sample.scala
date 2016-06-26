package jvmdbbroker.plugin

import scala.xml._

import jvmdbbroker.core._

class InitHookSample(val router:Router,val cfgNode: Node) extends Logging with InitHook {

  log.info("init hook created")

  def loadParameter(pmap:HashMapStringString):Unit = {
    log.info("loadParameter called")
  }
}
