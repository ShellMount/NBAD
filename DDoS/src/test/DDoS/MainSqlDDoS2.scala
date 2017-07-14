package DDoS

import org.apache.spark.sql.SparkSession

/**
  * Created by ShellMount on 2017/6/23.
  */
object MainSqlDDoS2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MainSqlDDoS")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()

    /**
      * JSON文件的要求：
      * 1， 必须是标准的双引号
      * 2， 不可以带有不被识别的对象，datetime, id
      * 3， 不支持 L 类型
      */

    val deepFlowDF = spark.read.json("D:\\WorkSpace\\APPS\\Source\\json\\1.json")
    deepFlowDF.printSchema()

    deepFlowDF.show()
    deepFlowDF.createOrReplaceTempView("deepFlowTable")


    // 编写对应的SQL语句在这里
    val sqlText = "SELECT count(DISTINCT dst_port) as cnt, src_ip, dst_ip, timestamp from deepFlowTable " +
      "GROUP BY src_ip, dst_ip, timestamp " +
      "ORDER BY timestamp, cnt " +
      "DESC"

    //val sqlText = "DESC deepFlowTable"
    val queryResultDF = spark.sql(sqlText)
    queryResultDF.show()

    val currDir = System.getProperty("user.dir")
    System.out.println("当前工作目录：" + currDir)

  }
}
