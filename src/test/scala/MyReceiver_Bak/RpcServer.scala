package MyReceiver

import java.io.{DataInputStream, DataOutputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}


/**
  * 模拟rpc通信的服务端程序
  * Created by Administrator on 2017/4/27 0027.
  */

/*case class SubmitTask(id: String, name: String)
case class HeartBeat(time: Long)
case object CheckTimeOutTask*/

object RpcServer {

  //val arr = Array(CheckTimeOutTask, new HeartBeat(123), HeartBeat(88888), new HeartBeat(666), SubmitTask("0001", "task-0001"))

  def main(args: Array[String]): Unit = {
    //创建服务端
    val port = 10001
    val listener: ServerSocket = new ServerSocket(port)
    println(s"REP Service is On.......PORT:${port}")

    var counter = 0
    while (true) {
      //建立socket通信
      val socket: Socket = listener.accept()
      val out = new DataOutputStream(socket.getOutputStream())
      val in = new ObjectInputStream(new DataInputStream(socket.getInputStream()))

      val read: String = in.readUTF()
      //println("Subscribed Topic：" + read)

      val command: Array[String] = read.split(" ")

      val result = command match {
        case Array("HeartBeat", time) => {
          s"$time"
        }
        case Array("SparkStreamingConsumer", "OriginalFlowData") => {
          /**
            flow_id: Long,
            net_src: String,
            ip_src: String,
            ip_dst: String,
            port_src: Int,
            port_dst: Int,
            tcp_flags_0: Int,
            start_time: Long,
            end_time: Long,
            duration: Int,
            proto: Int,
            total_byte_cnt_0: Long,
            total_pkt_cnt_0: Long
           **/
          var flows =
            """
               1234123411,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,555555555555,555555555615,33,7,88888888,99
               1234123412,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,66666666666,555555555615,33,7,88888888,99
               1234123413,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,77777777777,555555555615,33,7,88888888,99
               1234123414,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,88888888888,555555555615,33,7,88888888,99
               1234123415,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,99999999999,555555555615,33,7,88888888,99
               1234123416,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,111111111111,555555555615,33,7,88888888,99
               1234123417,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,222222222222,555555555615,33,7,88888888,99
               1234123418,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,333333333333,555555555615,33,7,88888888,99
               1234123419,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,444444444444,555555555615,33,7,88888888,99
               1234123420,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,5555555555555,555555555615,33,7,88888888,99
               1234123421,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,6666666666666,555555555615,33,7,88888888,99
            """.stripMargin
          flows = "2222222222,127.0.0.0,127.1.2.3,127.100.100.100,5677,3389,27,000000000000,11111111111,33,7,88888888," + counter.toString  + flows

          flows
        }
        case _ => {
          println("ERROR command: " + command)
          "error"
        }
      }

      counter += 1
      //将服务端的结果写回客户端
      out.writeUTF(result.toString)
      out.flush()

      //out.close()
      in.close()
      socket.close()
    }



  }

}
