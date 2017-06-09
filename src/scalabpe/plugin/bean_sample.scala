package scalabpe.plugin

import scala.xml._

import scalabpe.core._

class SampleBean(val router:Router,val cfgNode: Node) extends Logging with Bean {

    log.info("sample bean created")

}
