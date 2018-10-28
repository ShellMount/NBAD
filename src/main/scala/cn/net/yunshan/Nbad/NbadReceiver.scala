package cn.net.yunshan.Nbad

import java.io._
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source
import scala.util.control.Breaks._

class NbadReceiver(dataPath: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  println("Receiver is on")

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      //override def run() { receive() }
      override def run() { receiveFromFile() }
    }.start()
  }

  def onStop() {}

  /** Create a socket connection and receive data until receiver is stopped */
  private def receiveFromSocket() {
    var socket: Socket = null
    var userInput: String = null

    try {
      // Connect to host:port
      socket = new Socket("", 0)

      //发送一个请求
      //      val out: ObjectOutputStream = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream))
      //      val req = groupID
      //      out.writeUTF(req)
      //      out.flush()

      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }

      reader.close()
      //out.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      Thread.sleep(1000 * 20)
      restart("Finished Trying to connect again")
    } catch {
      case e: ConnectException =>
        restart("Error connecting to ZMQ server", e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }

  private def receiveFromFile(): Unit = {
    var lastInterval = ""
    try {
      while (true) {
        Thread.sleep(1000 * 1)
        val intervalParser = new SimpleDateFormat("yyyyMMddHHmm")
        val cal = Calendar.getInstance()
        cal.add(Calendar.MINUTE,-1)
        val interval = intervalParser.format(cal.getTime())
        breakable{
          if (lastInterval == interval) {
            break()
          } else {
            lastInterval = interval
          }

          restart("clean the store pool")
          val filePath =  s"${dataPath}${interval}"
          val file = Source.fromFile(filePath)
          val lines = file.getLines()
          for (line <- lines) {
            store(line)
          }

          file.close()
        }
      }
    } catch {
      case e: FileNotFoundException =>
        println(e)
    }

    Thread.sleep(1000 * 10)
    restart("Error to restart...")
  }
}
