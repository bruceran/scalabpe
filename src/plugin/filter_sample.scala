package jvmdbbroker.plugin

import scala.xml._

import jvmdbbroker.core._

class SampleRequestFilter(val router:Router,val cfgNode: Node) extends Logging with RequestFilter {

  log.info("sample request filter created")

  def filter(req: Request): Unit = {
    log.info("sample request filter called, req={}",req.toString)
  }
}

class SampleResponseFilter(val router:Router,val cfgNode: Node) extends Logging with ResponseFilter {

  log.info("sample response filter created")

  def filter(res: Response,req: Request): Unit = {
    log.info("sample response filter called, res={}",res.toString)
  }

}
