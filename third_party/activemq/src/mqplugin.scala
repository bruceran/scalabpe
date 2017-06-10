package scalabpe.plugin

import scalabpe.core._

// selialize body to string
trait MqSelialize {
    def selialize(serviceId: Int, msgId: Int, body: HashMapStringAny): String
}

// parse contentStr into body and return msgId
// return -1 means using the default method
trait MqDeselialize {
    def deselialize(queueName: String, contentStr: String, body: HashMapStringAny): Int
}
