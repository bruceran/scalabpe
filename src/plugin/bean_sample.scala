package jvmdbbroker.plugin

import scala.xml._

import jvmdbbroker.core._

class SampleBean(val router:Router,val cfgNode: Node) extends Logging with Bean {

    log.info("sample bean created")

}
