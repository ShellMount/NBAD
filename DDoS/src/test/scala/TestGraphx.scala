package scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * Created by ShellMount on 2017/7/9.
  */
object TestGraphx {
  def main(args: Array[String]): Unit = {
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

    val conf = new SparkConf().setMaster("local[4]").setAppName("MainGraphx")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val vertexRDD: RDD[(Long, (String, String, String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val telecommGraph: Graph[(String, String, String, Int), Int] = Graph(vertexRDD, edgeRDD)

    // 访问图中的数据
    telecommGraph.vertices.collect.foreach(println)

    telecommGraph.edges.collect.foreach(println)

    telecommGraph.triplets.collect.foreach(println)


    println("边总数: " + telecommGraph.numEdges)
    println("顶总数: " + telecommGraph.numVertices)
    println("总入度: " + telecommGraph.inDegrees)
    println("总出度: " + telecommGraph.outDegrees)
    println("图总度: " + telecommGraph.degrees)


    println("==" * 30 + "求子图")

    println("通话次数大于5的顶点及边，并构成新图")
    val moreThanFiveContactGraph = telecommGraph.subgraph(epred = e => e.attr > 5)
    moreThanFiveContactGraph.triplets.collect.foreach(println)

    println("双方均为联通用户，且至少通话 1 次")
    val bothUnicomGraph = telecommGraph.subgraph(vpred = (id, vd) => vd._2 == "Unicom", epred = e => e.attr > 1)
    bothUnicomGraph.triplets.collect.foreach(println)


    println("==" * 30 + "属性转换")
    println("转换后，过滤掉一些属性，保留有用的属性")

    // 只保留用户的年龄
    val userAgeGraph = telecommGraph.mapVertices((id, vd) => vd._4)
    userAgeGraph.vertices.collect.foreach(println)


    println("==" * 30 + "邻边聚合")

    println("发现年长的联络人,由源节点向目标节点发送消息，并对人数和年龄求和")
    val olderCaller = userAgeGraph.aggregateMessages[(Int, Int)](triplet => {
      if (triplet.srcAttr > triplet.dstAttr) {
        triplet.sendToDst(1, triplet.srcAttr)
      }
    },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )

    println("年长者呼叫人汇总： ")
    println("(ID, (呼叫总人数, 呼叫总岁数))" + "\n" + "--" * 15)
    olderCaller.collect.foreach(println)


    println("求平均年龄")
    val avgAgeOfOlderCallers = olderCaller.mapValues((id, value) => {
      value match {
        case (count, totalAge) => totalAge / count
      }
    })
    avgAgeOfOlderCallers.collect.foreach(println)


    println("==" * 30 + "内部分析函数")
    // 精确度： 0.00001
    val pageRanks = telecommGraph.pageRank(0.00001).vertices
    println("ID, rank")
    pageRanks.collect.foreach(println)

    // 与顶点图 JOIN 获取姓名
    val userRanks = vertexRDD.join(pageRanks).map{
      case (id, (userInfo, pageRank)) => (userInfo._1, pageRank)
    }

    println("--" * 15)
    println("用户, rank  <-- 求出用户对应的权重" )
    userRanks.collect.foreach(println)



  }
}
