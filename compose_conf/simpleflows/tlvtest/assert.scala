package scalabpe.flow

import scalabpe.core._
import scalabpe.flow.FlowHelper._

abstract class AssertFlow extends Flow {

    var assert_result_code = 0

    def assertTrue(v:Boolean) {
        if( true != v) { output(true,v) }
    }
    def assertFalse(v:Boolean) {
        if( false != v) { output(false,v) }
    }
    def assertEquals(r:Int,v:Any) {
        if( r != v) { output(r,v) }
    }
    def assertEquals(r:String,v:Any) {
        if( r != v) { output(r,v) }
    }

    def output(r:Any,v:Any) {
        val (clsName,lineNumber) = getLocation()
        val m = "assert failed, "+clsName+":"+lineNumber+" required:"+r+", actually:"+v
        println(m)
        assert_result_code -= 1
    }

    def getLocation():Tuple2[String,String] = {
        val elements = Thread.currentThread().getStackTrace()
        for (e <- elements) {
            val clsName = e.getClassName()
            val methodName = e.getMethodName()
            val fileName = e.getFileName()
            val lineNumber = e.getLineNumber()
            if (clsName.indexOf("scalabpe.flow.") >= 0 && clsName.indexOf("AssertFlow") < 0 ) {
                return (clsName,lineNumber.toString)
            }
        }
        ("","")
    }
    
}

