//package MyReceiver
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.{Durations, StreamingContext}
//
//object MyStreaming {
//  def main(args: Array[String]): Unit = {
//    val host = "127.0.0.1"
//    val port = 10001
//    val groupId = "SparkStreamingConsumer"
//    val topic = "OriginalFlowData"
//
//    val spark = SparkSessionSingleton.getInstance("xxx")
//    val conf = spark.sparkContext
//    conf.setLogLevel("WARN")
//
//    val ssc = new StreamingContext(conf, Durations.seconds(10))
//    val stream = ssc.receiverStream(
//      new CustomReceiver(
//        host,
//        port,
//        groupId,
//        topic))
//    val formatedLines = stream.map(x => x.split(",")).filter(_.length > 4)
//
//    formatedLines.foreachRDD(
//      _rdd => {
//        val sqlContext = SQLContext.getOrCreate(_rdd.sparkContext)
//        import sqlContext.implicits._
//        val df = _rdd.map(record =>
//          Flow(
//            record(0).toString.trim,
//            record(1).toString,
//            record(2).toString,
//            record(3).toString,
//            record(4).toInt,
//            record(5).toInt,
//            record(6).toInt,
//            record(7).toLong,
//            record(8).toLong,
//            record(9).toInt,
//            record(10).toInt,
//            record(11).toLong,
//            record(12).toLong
//          )
//        ).toDF
//        val table = df.createOrReplaceTempView("OriginalFlow")
//        val sqlText = "select * from OriginalFlow "
//        val df2=sqlContext.sql(sqlText)
//        df2.show()
//      }
//    )
//
//
//    ssc.start()
//
//    ssc.awaitTermination()
//
//    ssc.stop()
//  }
//
//}
