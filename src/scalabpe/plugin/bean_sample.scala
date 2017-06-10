package scalabpe.plugin

import scala.xml.Node

import scalabpe.core.Bean
import scalabpe.core.Logging
import scalabpe.core.Router

class SampleBean(val router: Router, val cfgNode: Node) extends Logging with Bean {

    log.info("sample bean created")

}
