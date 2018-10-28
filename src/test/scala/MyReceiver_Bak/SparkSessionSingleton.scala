package MyReceiver_Bak

import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  @transient  private var instance: SparkSession = _

  def getInstance(): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .appName("SparkStreamingForNBAD")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    }
    instance
  }
}
