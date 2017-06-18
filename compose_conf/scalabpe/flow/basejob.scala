package scalabpe.flow

import scalabpe.core._
import scalabpe.flow.FlowHelper._

abstract class BaseJobFlow extends Flow {

    var jobId = ""
    var jobRunning = false

    override def baseReceive():Unit = {

println("start ... ")
        if( jobId != "" ) {
            val running = jobStatusCache.get(jobId)
            if( running != null ) {
println(" is running  ... ")
                reply(0)
                return
            }
            jobStatusCache.put(jobId,"1")
            jobRunning = true
        }
        receive()
    }

    override def baseEndFlow() :Unit = {
        if( jobId != "" && jobRunning ) {
println("remove job ")
            jobStatusCache.remove(jobId)
        }
    }
    
}

