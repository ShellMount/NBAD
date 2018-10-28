package cn.net.yunshan.Nbad

import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  @transient  private var instance: SparkSession = _

  def getInstance(conf: ConfigFile): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .appName("SparkStreamingForNBAD")
        .config("es.nodes", conf.getItemAsString("es_nodes"))
        .config("es.port", conf.getItemAsInt("es_port"))
        .config("es.index.auto.create", true)
        .getOrCreate()
    }

    instance
  }
}
