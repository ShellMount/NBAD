package zmq

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.zeromq.{Bind, ZMQMessage, ZeroMQExtension}

object SimpleZeroMQPublisher {
  def main(args: Array[String]): Unit = {
//    if (args.length < 2) {
//      System.err.println("Usage: SimpleZeroMQPublisher <zeroMQUrl> <topic> ")
//      System.exit(1)
//    }

    val url = "127.0.0.1:3000"
    val topic = "ZMQ"
    //val Seq(url, topic) = args.toSeq
    val acs: ActorSystem = ActorSystem()

    val pubSocket = ZeroMQExtension(acs).newPubSocket(Bind(url))
    implicit def stringToByteString(x: String): ByteString = ByteString(x)
    val messages: List[ByteString] = List("words ", "may ", "count ")
    while (true) {
      Thread.sleep(1000)
      pubSocket ! new ZMQMessage(ByteString(topic) :: messages)
      println("发送了一条消息")
    }
    acs.awaitTermination()
  }
}
