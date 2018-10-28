//package MyReceiver
//
//import org.apache.log4j.Logger
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//import scala.collection.mutable
//import org.elasticsearch.spark.sql._
//
//class DataAnalysis(logger: => Logger, spark: SparkSession, schemaFlow: DataFrame, interval: String, phase: String, conf: ConfigFile, intervalDate: Long) {
//  private val _spark = spark
//  var _feature: mutable.Map[String, Double]// = new mutable.Map<String, Double>
//  var _checkResults: mutable.Map[String, StatusEnum.Value]
//  var _featureRecords: Array[(String, Long, Double, Double, Double, Double, Double)]
//  private val history = new FeatureHistory(conf)
//
//  def featureExtract: Unit = {
//    logger.info("Calculate features of datas")
//    _feature = EntropyFeature.calFeatures(schemaFlow, conf.getItemAsArray("feature_keys"))
//  }
//
//  def historyCheck: Unit = {
//    logger.info("Load history from history_path")
//    history.load(conf.getItemAsString("history_path"), conf.getItemAsArray("feature_keys"))
//    logger.info("Check features with history")
//    var (_checkResults, _featureRecords) = history.check(intervalDate, phase, _feature)
//    //_checkResults = history.check(intervalDate, phase, _feature)(0)
//    //_featureRecords = history.check(intervalDate, phase, _feature)(1)
//  }
//
//  def saveFeatures: Unit = {
//    logger.info("Record features into elasticsearch")
//    import _spark.implicits._
//    Seq(_featureRecords: _*).
//      toDF("key", "timestamp", "feature", "lower", "upper", "bottom", "ceiling").
//      saveToEs("history/features")
//  }
//
//
//  def takeAction: Unit = {
//    _checkResults.values.max match {
//      case StatusEnum.normal => {
//        logger.info("Normal, update history")
//        history.update(_feature)
//        history.save(conf.getItemAsString("history_path"))
//      }
//      case StatusEnum.malicious => {
//        logger.info("Malicious, detective attacker")
//        Detector.findAttacker(schemaFlow, _checkResults, intervalDate, conf)
//      }
//    }
//  }
//}
