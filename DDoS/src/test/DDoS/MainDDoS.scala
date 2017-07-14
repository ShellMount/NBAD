package DDoS

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by ShellMount on 2017/6/23.
  */

object MainDDoS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DDoSMessageFlowDealing")
    conf.setMaster("local[3]")

    //val sc = new SparkContext()
    //sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(conf, Durations.seconds(10))

    //val kafkaParameters= Map("metadata.broker.list" -> "kafkamaster:9092")
    val kafkaParameters= Map("metadata.broker.list" -> "localhost:9092")

    val topics = Set("DDoSMessageFlow")

    // 这里的 createDirectStream 需要指定类型
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParameters,
      topics)

    println("继续")

    lines.foreachRDD(rdd => rdd.foreach(println))


    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
