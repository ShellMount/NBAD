package zmq

import org.zeromq.ZMQ

object zmqPull {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SimpleZeroMQPublisher <zeroMQUrl> <topic> ")
      System.exit(1)
    }

    val Seq(url, topic) = args.toSeq
    val phase = if (args.length == 2) args(1).toString else "analyzing"
    val context = ZMQ.context(1)
    val pull = context.socket(ZMQ.PULL)
    pull.connect(url)
    while (true) {
      println("建立连接成功")
      val message = new String(pull.recv(1))
      //val message = pull.recv(1)

      println("received message: " + message)
    }
  }
}
