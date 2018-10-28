package zmq

import org.zeromq.ZMQ


object zmqPush {
  def main(args: Array[String]): Unit = {

    val context = ZMQ.context(1)
    val push = context.socket(ZMQ.PUSH)
    push.bind("127.0.0.1:9999")

    var i = 0
    while ( {i < 10}) {
      push.send(Array(0, 1, 2),  i)
      i += 1
      println("发送消息 : " + i)
    }

    push.close()
  }
}
