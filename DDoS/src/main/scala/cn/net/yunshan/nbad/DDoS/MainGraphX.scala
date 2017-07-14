package cn.net.yunshan.nbad.DDoS

import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession

/**
  * Created by ShellMount on 2017/7/3.
  */
object MainGraphX {
  def main(args: Array[String]): Unit = {
    // 顶点，就是对象
    val vertexArray = Array(
      (1L, ("Zhang Fei", "Unicom", "Male", 36)),
      (2L, ("Li Zhi", "CMCC", "Female", 18)),
      (3L, ("He Ruina", "Unicom", "Male", 23)),
      (4L, ("Dong Xicheng", "CMCC", "Male", 42)),
      (5L, ("Bai Zi", "Unicom", "Male", 25)),
      (6L, ("Fu Ming", "Unicom", "Male", 50)),
      (7L, ("Handel", "ATT", "Male", 78)),
      (8L, ("Carmen", "ATT", "Female", 21))
    )

    // 边界，就是关联性，关系
    val edgeArray = Array(
      Edge(2L, 1L, 4),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 5),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 2),
      Edge(7L, 6L, 9),
      Edge(7L, 3L, 8),
      Edge(8L, 7L, 3),
      Edge(8L, 6L, 2)
    )

    /*val conf = new SparkConf().setAppName("MainGraphX").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
    */



    val spark = SparkSession
          .builder
          .appName("Python Spark SQL basic example")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val df = spark.read.json("D:\\WorkSpace\\APPS\\Source\\json\\5.json")
    // {"tcp_flags": 2, "src_ip": "192.168.0.3", "direction": 0, "used": 2569089111, "src_port": 15000, "dst_ip": "104.202.139.246", "byte_count": 1024, "timestamp": 1495745519, "datapath": 603984300, "packet_count": 1, "eth_type": 2048, "dst_port": 80, "dst_mac": "b6:8c:66:16:f8:07", "time": "2017, 5, 25, 20, 51, 59", "duration": 0, "protocol": 6, "src_mac": "52:54:00:e1:f0:d5", "vlantag": 5144}

    val ttime = df.show()

  }

}
