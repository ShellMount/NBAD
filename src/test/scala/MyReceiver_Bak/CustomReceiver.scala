package MyReceiver


import java.io._
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomReceiver(host: String, port: Int, groupID: String, topic: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  println("Subscribing Topic: " + topic)

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null

    try {
      // Connect to host:port
      socket = new Socket(host, port)

      //发送一个请求
      val out: ObjectOutputStream = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream))
      val req = groupID + " " + topic
      out.writeUTF(req)
      out.flush()

      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()



      //接受服务器结果
//      val in = new DataInputStream(socket.getInputStream)
//      userInput = in.readUTF()
//
//      if(!isStopped && userInput != null) {
//        store(userInput)
//      }
//      in.close()
      out.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      Thread.sleep(1000 * 15)
      restart("Finished Trying to connect again")
    } catch {
      case e: ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
