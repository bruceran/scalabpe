package scalabpe.flow

import scalabpe.core._
import scalabpe.flow.FlowHelper._
import scala.collection.mutable.ArrayBuffer

class Flow_service999_test222 extends Flow {

    def receive() {
        invoke(callback, "chatdb.insert", 3000,
            "id" -> 123,
            "name" -> "test",
            "names" -> ArrayBufferString("t1", "t2"),
            "nickName" -> "nickaname 111")
    }
    
    def callback() {
        val ret = lastresult()
        reply(ret.code)

    }

}

