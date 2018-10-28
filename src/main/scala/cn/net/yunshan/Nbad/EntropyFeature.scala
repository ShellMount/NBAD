package cn.net.yunshan.Nbad

import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.math.log


object EntropyFeature {
  def calFeatures(schemaFlow: DataFrame, featureKeys: Array[String]): mutable.Map[String, Double] = {
    @transient lazy val logger = org.apache.log4j.LogManager.getLogger(EntropyFeature.getClass)
    val features: Map[String, Double] = Map()
    val totalCount = schemaFlow.count()
    val logTotalCount = log(totalCount)

    logger.info(s"Total count of datas: $totalCount")
    for (key <- featureKeys) {
      totalCount match {
        case 0 => features(key) = 0
        case _ => {
          val schemaGroupByKey = schemaFlow.select(key).groupBy(key).count().cache()
          val columnCount = schemaGroupByKey.count()
          val logColumnCount = log(columnCount)

          // FIXME: remove rdd in the feature
          val entropy = schemaGroupByKey.select("count").rdd.
            map(count => -(log(count.getLong(0)) - logTotalCount) * count.getLong(0)).
            reduce(_ + _)
          features(key) = entropy / logColumnCount / totalCount
        }
      }
    }
    features
  }
}
