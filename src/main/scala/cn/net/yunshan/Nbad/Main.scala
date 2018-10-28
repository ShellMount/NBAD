package cn.net.yunshan.Nbad

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.spark.sql._


object Main {
  def main(args: Array[String]): Unit = {
    @transient lazy val logger = org.apache.log4j.LogManager.getLogger(Main.getClass)
    val phase = if (args.length == 1) args(1).toString else "analyzing"
    var configFile: String = null
    val os = System.getProperties.getProperty("os.name")
    if (os.toUpperCase.contains("WINDOWS")) {
      // for local
      configFile = "E:\\APP\\config\\nbad.properties"
    } else {
      configFile = "/data/nbad/conf/nf/nbad.properties"
    }

    val conf = new ConfigFile(configFile)
    val dataPath = conf.getItemAsString("data_path")

    val Spark = SparkSessionSingleton.getInstance(conf)
    val sc = Spark.sparkContext
    val ssc = new StreamingContext(sc, Durations.seconds(conf.getItemAsInt("durations")))
    val stream = ssc.receiverStream(new NbadReceiver(dataPath))

    // data checking
    val formatedLines = stream.map(x => x.split(","))
      .filter(line => line.length > 12 && ! line.contains(""))

    formatedLines.foreachRDD(
      _rdd => {
        val sqlContext = SQLContext.getOrCreate(_rdd.sparkContext)
        import sqlContext.implicits._
        val df = _rdd.map(record =>
          Flow(
            record(0).toString,
            record(1).toString,
            record(2).toString,
            record(3).toString,
            record(4).toString,
            record(5).toInt,
            record(6).toInt,
            record(7).toInt,
            record(8).toLong,
            record(9).toLong,
            record(10).toLong,
            record(11).toInt,
            record(12).toLong,
            record(13).toLong,
            record(14).toInt
          )
        ).toDF
        val table = df.createOrReplaceTempView("OriginalFlow")
        println (s"count data, items of ${df.count()}")
        df.show()

        println("本次分析开始时间: " + new Date())
        // 计算分析
        if (df.count() > 0) {
          val interval = df.select("interval").first().get(0).toString
          val intervalParser = new SimpleDateFormat("yyyyMMddHHmm")
          val intervalDate = intervalParser.parse(interval).getTime / 1000
          logger.warn("current interval: " + interval)

          logger.info("Calculate features of datas")
          val features = EntropyFeature.calFeatures(df, conf.getItemAsArray("feature_keys"))

          logger.info("Check features with history")
          val history = new FeatureHistory(conf)
          history.load(conf.getItemAsString("history_file"), conf.getItemAsArray("feature_keys"))
          val (checkResults, featureRecords) = history.check(intervalDate, phase, features)

          try {
            logger.info("Record features into elasticsearch")
            Seq(featureRecords: _*)
              .toDF("key", "timestamp", "feature", "lower", "upper", "bottom", "ceiling")
              .saveToEs("history/features")
          } catch {
            case e: EsHadoopIllegalArgumentException =>
              logger.error("Error in save to ES. " + e)
            case t: Throwable =>
              logger.error("Error in save to ES. " + t)
          }

          try {
            checkResults.values.max match {
              case StatusEnum.normal => {
                logger.info("Normal, update history")
                history.update(features)
                history.save(conf.getItemAsString("history_file"))
              }
              case StatusEnum.malicious => {
                logger.info("Malicious, detective attacker")
                Detector.findAttacker(df, checkResults, intervalDate, conf)
              }
            }
          } catch {
            case t: Throwable =>
              logger.error("Error in CheckResults. " + t)
          }
        }

        println("本次分析结束时间: " + new Date())
      }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
