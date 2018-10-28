package cn.net.yunshan.Nbad

import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession

import org.elasticsearch.spark.sql._

object Nbad_bak {
  def main(args: Array[String]) {
    @transient lazy val logger = org.apache.log4j.LogManager.getLogger(Nbad_bak.getClass)
    val interval = if (args.length >= 1) args(0).toString else return
    val phase = if (args.length == 2) args(1).toString else "analyzing"
    val conf = new ConfigFile("/etc/nbad.properties")
    val intervalParser = new SimpleDateFormat("yyMMddHHmm")
    val intervalDate = intervalParser.parse(interval).getTime / 1000

    logger.info(s"Start $phase $interval.parquet")
    val spark = SparkSession
      .builder
      .appName("nbad YunShan")
      .getOrCreate()

    logger.info("Load datas from data_path")
    val schemaFlow = spark.read.parquet(conf.getItemAsString("data_path") + interval + ".parquet")

    logger.info("Calculate features of datas")
    val features = EntropyFeature.calFeatures(schemaFlow, conf.getItemAsArray("feature_keys"))

    logger.info("Load history from history_path")
    val history = new FeatureHistory(conf)
    history.load(conf.getItemAsString("history_path"), conf.getItemAsArray("feature_keys"))

    logger.info("Check features with history")
    val (checkResults, featureRecords) = history.check(intervalDate, phase, features)

    logger.info("Record features into elasticsearch")
    import spark.implicits._
    Seq(featureRecords: _*).
      toDF("key", "timestamp", "feature", "lower", "upper", "bottom", "ceiling").
      saveToEs("history/features")

    checkResults.values.max match {
      case StatusEnum.normal => {
        logger.info("Normal, update history")
        history.update(features)
        history.save(conf.getItemAsString("history_path"))
      }
      case StatusEnum.malicious => {
        logger.info("Malicious, detective attacker")
        Detector.findAttacker(schemaFlow, checkResults, intervalDate, conf)
      }
    }

    logger.info(s"Stop $phase $interval.parquet")
    spark.stop()
  }
}
