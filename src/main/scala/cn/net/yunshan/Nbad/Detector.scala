package cn.net.yunshan.Nbad

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.elasticsearch.spark.sql._

import scala.collection.mutable


object Detector {
  def findAttacker(schemaFlow: DataFrame, checkResults: mutable.Map[String, StatusEnum.Value], interval: Long, conf: ConfigFile): Unit = {
    @transient lazy val logger = org.apache.log4j.LogManager.getLogger(Detector.getClass)
    val indexInterval = new SimpleDateFormat("yyMMdd").format(interval * 1000)
    val indexName = s"nbad_$indexInterval"

    logger.debug("Detect port scan")
    val portScanThreshold = conf.getItemAsInt("port_scan_threshold")
    schemaFlow.groupBy("ip_src", "ip_dst").
      agg(countDistinct("port_dst").alias("quantity")).
      filter(s"quantity > $portScanThreshold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_port_scan/feature")

    logger.debug("Detect range scan")
    val rangeScanThreshold = conf.getItemAsInt("range_scan_threshold")
    schemaFlow.groupBy("ip_src", "port_dst").
      agg(countDistinct("ip_dst").alias("quantity")).
      filter(s"quantity > $rangeScanThreshold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_range_scan/feature")

    logger.debug("Detect tcp flood")
    val tcpFloodThreshold = conf.getItemAsInt("tcp_flood_threshold")
    schemaFlow.filter("proto = 6").
      groupBy("ip_src", "ip_dst", "tcp_flags_0").
      agg(count("*").alias("quantity")).
      filter(s"quantity> $tcpFloodThreshold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + s"_tcp_flood/feature")

    logger.debug("Detect udp flood")
    val udpFloodThrehold = conf.getItemAsInt("udp_flood_threhold")
    schemaFlow.filter("proto = 17").
      groupBy("ip_src", "ip_dst").
      agg(sum("total_byte_cnt_0").alias("quantity")).
      filter(s"quantity > $udpFloodThrehold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_udp_flood/feature")

    logger.debug("Detect dns/ssl/sip flood")
    val dssFloodThrehold = conf.getItemAsInt("dss_flood_threhold")
    schemaFlow.groupBy("ip_src", "ip_dst", "port_dst").
      agg(sum("total_pkt_cnt_0").alias("quantity")).
      filter(s"quantity > $dssFloodThrehold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_dss_flood/feature")

    logger.debug("Detect http flood")
    val httpFloodThreshold = conf.getItemAsInt("http_flood_threshold")
    schemaFlow.filter("port_dst = 80 and (tcp_flags_0 = 27 or tcp_flags_0 = 31)").
      groupBy("ip_src", "ip_dst").
      agg(count("*").alias("quantity")).
      filter(s"quantity > $httpFloodThreshold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_http_flood/feature")

    logger.debug("Detect snmp reflection")
    val snmpReflectionThreshold = conf.getItemAsInt("snmp_reflection_threshold")
    schemaFlow.filter("port_src = 80 and port_dst = 161 and proto = 17").
      groupBy("ip_src").
      agg(count("*").alias("quantity")).
      filter(s"quantity > $snmpReflectionThreshold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_snmp_reflection/feature")

    logger.debug("Detect ping flood")
    val pingFloodThrehold = conf.getItemAsInt("ping_flood_threshold")
    schemaFlow.filter("proto = 1 and port_src = 8").
      groupBy("ip_src", "ip_dst").
      agg(sum("total_pkt_cnt_0").alias("quantity")).
      filter(s"quantity> $pingFloodThrehold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_ping_flood/feature")

    logger.debug("Detect land")
    schemaFlow.filter(flow => {
      (flow.getAs[String]("ip_src") != null) &&
        (flow.getAs[String]("port_src") != null) &&
        (flow.getAs[String]("ip_src") != "0.0.0.0") &&
        (flow.getAs[String]("ip_src") == flow.getAs[String]("ip_dst")) &&
        (flow.getAs[String]("port_src") == flow.getAs[String]("port_dst"))
    }).withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_land/feature")

    var end255 = schemaFlow.
      filter(flow => {
        val ipDst = flow.getAs[String]("ip_dst")
        if (ipDst == null) false else ipDst.split("\\.")(3) == "255"
      }).
      cache()

    logger.debug("Detect smurf")
    val smurfThrehold = conf.getItemAsInt("smurf_threshold")
    end255.filter("proto = 1").groupBy("ip_src", "ip_dst").
      agg(sum("total_pkt_cnt_0").alias("quantity")).
      filter(s"quantity > $smurfThrehold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_smurf/feature")

    logger.debug("Detect fraggle")
    val fraggleThrehold = conf.getItemAsInt("fraggle_threshold")
    end255.filter("proto = 17 and " +
      "((port_dst >= 1024 and port_dst <= 5000) or " +
      "(port_dst >= 30000 and port_dst <= 40000) or " +
      "port_dst in (5, 19, 37, 53, 88, 111, 123, 177, 443, 520, 635, 640, 731, 733, 749))").
      groupBy("ip_src", "ip_dst").
      agg(sum("total_pkt_cnt_0").alias("quantity")).
      filter(s"quantity > $fraggleThrehold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_fraggle/feature")

    logger.debug("Detect similar flows")
    var groupKeys: Array[Column] = Array()
    for (key <- checkResults.keys) {
      checkResults(key) match {
        case StatusEnum.normal =>
        case _ => groupKeys = groupKeys :+ schemaFlow(key)
      }
    }
    val similarFlowThreshold = conf.getItemAsInt("similar_flows")
    schemaFlow.groupBy(groupKeys: _*).
      agg(count("*").alias("quantity")).
      filter(s"quantity > $similarFlowThreshold").
      withColumn("timestamp", lit(interval)).
      saveToEs(indexName + "_similar_flows/feature")
  }
}
