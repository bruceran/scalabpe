
package scalabpe.flow;

import scalabpe.core._
import FlowHelper._

class Flow_service_t2 extends Flow { 

    def receive() {

      val list = ArrayBufferMap()

      val m = HashMapStringAny(
        "book_id"->"123",
        "game_id"->12345 )

      val m2 = HashMapStringAny(
        "book_id"->"456",
        "game_id"->12345 )

      list += m
      list += m2

      reply(0,"list"->m)

    }

}
