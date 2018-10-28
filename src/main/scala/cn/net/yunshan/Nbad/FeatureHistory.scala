package cn.net.yunshan.Nbad

import java.io.{File, PrintWriter}

import MyReceiver.EntropyVector

import scala.collection.mutable.Map
import scala.io.Source

class FeatureHistory(conf: ConfigFile) {
  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(getClass)
  val vectors: Map[String, EntropyVector] = Map()
  var count: Double = 0.0
  var suspiciousRate: Int = conf.getItemAsInt("suspicious_rate")
  var maliciousRate: Int = conf.getItemAsInt("malicious_rate")

  def load(path: String, featureKeys: Array[String]): Unit = {
    count = 0.0
    for (key <- featureKeys) {
      vectors(key) = new EntropyVector()
    }

    try {
      val reader = Source.fromFile(path)
      for (line <- reader.getLines()) {
        val content = line.split(":")
        content(0) match {
          case "count" => count = content(1).toDouble
          case _ => {
            var values = content(1).split(",")
            values.length match {
              case 4 => vectors(content(0)).setValues(values(0).toDouble, values(1).toDouble, values(2).toDouble, values(3).toDouble)
            }
          }
        }
      }
      reader.close()
    } catch {
      case e: java.io.FileNotFoundException => logger.warn("history file is not exists")
    }
  }

  def save(path: String): Unit = {
    val writer = new PrintWriter(new File(path))
    writer.write(s"count:$count\n")
    for (vector <- vectors.keys) {
      val vectorString = vectors(vector).toString
      writer.write(s"$vector:$vectorString\n")
    }
    writer.close()
  }

  def check(interval: Long, phase: String, features: Map[String, Double]):
  (Map[String, StatusEnum.Value], Array[(String, Long, Double, Double, Double, Double, Double)]) = {
    val checkResults: Map[String, StatusEnum.Value] = Map()
    var featureRecords: Array[(String, Long, Double, Double, Double, Double, Double)] = Array()

    for (key <- features.keys) {
      phase match {
        case "learning" => checkResults(key) = StatusEnum.withName("normal")
        case _ => {
          val lower = vectors(key).avg - suspiciousRate * vectors(key).stddev
          val upper = vectors(key).avg + suspiciousRate * vectors(key).stddev
          val bottom = vectors(key).avg - maliciousRate * vectors(key).stddev
          val ceiling = vectors(key).avg + maliciousRate * vectors(key).stddev
          featureRecords = featureRecords :+ (key, interval, features(key), lower, upper, bottom, ceiling)

          if (features(key) >= lower && features(key) <= upper) {
            checkResults(key) = StatusEnum.withName("normal")
          } else if (features(key) <= lower || features(key) >= upper) {
            checkResults(key) = StatusEnum.withName("malicious")
          } else {
            checkResults(key) = StatusEnum.withName("suspicious")
          }
        }
      }
    }
    (checkResults, featureRecords)
  }

  def update(features: Map[String, Double]): Unit = {
    count += 1
    for (key <- features.keys) {
      vectors(key).sum += features(key)
      count match {
        case 1 => vectors(key).variance = 0
        case _ => vectors(key).variance = (count - 2) / (count - 1) * vectors(key).variance + math.pow(features(key) - vectors(key).avg, 2) / count
      }
      vectors(key).avg = vectors(key).sum / count
      vectors(key).stddev = math.sqrt(vectors(key).variance)
    }
  }
}
