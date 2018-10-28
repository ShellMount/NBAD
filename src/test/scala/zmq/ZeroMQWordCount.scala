package zmq

import akka.util.ByteString
import akka.zeromq.Subscribe
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.zeromq.ZeroMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ZeroMQWordCount {
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: ZeroMQWordCount <zeroMQurl> <topic>")
//      System.exit(1)
//    }

    val url = "127.0.0.1:3000"
    val topic = "ZMQ"

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    //val Seq(url, topic) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ZeroMQWordCount")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    def bytesToStringIterator(x: Seq[ByteString]): Iterator[String] = x.map(_.utf8String).iterator

    // For this stream, a zeroMQ publisher should be running.
    val lines = ZeroMQUtils.createStream(
      ssc,
      url,
      Subscribe(topic),
      bytesToStringIterator _)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
