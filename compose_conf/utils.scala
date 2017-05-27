package scalabpe.flow

import scalabpe.core._
import java.util.Random

object Global {
    def init() {
        println("init called")
    }
    def close() {
        println("close called")
    }
}

object FlowHelper { 

    val random = new Random()
	
    val jobStatusCache = new java.util.concurrent.ConcurrentHashMap[String,String]()

    def getConfig(s:String,defaultValue:String="") = Flow.router.getConfig(s,defaultValue)

    def isEmpty(req:Request,name:String):Boolean={
        if( name.indexOf(",") >= 0 ) return isEmptyForAny(req,name)
        return isEmpty(req.s(name))
    }

    private def isEmptyForAny(req:Request,names:String):Boolean={
        val ss = names.split(",")
        var i = 0
        while( i < ss.length ) {
            if( isEmpty(req.s(ss(i)) )) return true
            i += 1
        }
        false
    }

    def isInt(req:Request,name:String):Boolean={
        if( name.indexOf(",") >= 0 ) return isIntForAny(req,name)
        return isInt(req.s(name))
    }    
	
    private def isIntForAny(req:Request,names:String):Boolean={
        val ss = names.split(",")
        var i = 0
        while( i < ss.length ) {
            if( isInt(req.s(ss(i)) )) return true
            i += 1
        }
        false
    }

    def isEmpty(str:String):Boolean={
        return str == null || str.length() == 0
    }

    def isInt(n:String):Boolean={
        try {
            Integer.parseInt(n)
            return true
        } catch {
            case e: Throwable =>
             return false
        }
    }

    def checkInclude(ss:String,s:String,t:String=","):Boolean={
        if( ss == null || ss == "" ) return false
        if( s == null || s == "" ) return true
        return (t+ss+t).indexOf(t+s+t) >= 0 
    }

    def uuid(): String = {
        return java.util.UUID.randomUUID().toString().replaceAll("-", "")
    }

    def generateSeed():String = {
        "%08d".format(Math.abs(random.nextInt())%100000000)
    }
 
    def contact(a:String,b:String):String = a + b
}



