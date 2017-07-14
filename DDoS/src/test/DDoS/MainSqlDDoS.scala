package DDoS

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ShellMount on 2017/6/23.
  */
object MainSqlDDoS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("MainSqlDDoS")
    conf.setMaster("local[3]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * JSON文件的要求：
      * 1， 必须是标准的双引号
      * 2， 不可以带有不被识别的对象，datetime, id
      * 3， 不支持 L 类型
      */
    val deepFlowDF = sqlContext.read.json("D:\\WorkSpace\\APPS\\Source\\testJson.json")
    deepFlowDF.registerTempTable("deepFlowTable")
    deepFlowDF.show()

    // 编写对应的SQL语句在这里
    val sqlText = "SELECT byte_count, dst_ip, dst_port " +
      " FROM deepFlowTable " +
      " LIMIT 10"

    //val sqlText = "DESC deepFlowTable"
    val sqlExecResultDF = sqlContext.sql(sqlText)

    sqlExecResultDF.show()
    val resultList = sqlExecResultDF.rdd.collect()

    for (item <- resultList) {
      println("ITEM : " + item)

    }

    println(sqlExecResultDF)

    val currDir = System.getProperty("user.dir")
    System.out.println("当前工作目录：" + currDir)

  }
}
