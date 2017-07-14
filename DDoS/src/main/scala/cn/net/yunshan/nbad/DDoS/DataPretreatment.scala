package cn.net.yunshan.nbad.DDoS

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by ShellMount on 2017/7/13.
  */
class DataPretreatment {

  def getSvm(sc: SparkContext, filePath: String): (RDD[LabeledPoint], RDD[LabeledPoint], RDD[LabeledPoint]) = {
    // 手工加载
    /*val data = MLUtils.loadLabeledPoints(sc,"D://data.txt")			    //读取数据集
      val data = sc.text("D://data.txt").map { line =>							//处理数据
      val parts = line.split(',')									                  //分割数据
      LabeledPoint(parts(0).toDouble, 							                //标签数据转换
        Vectors.dense(parts(1).split(' ').map(_.toDouble)))				  //向量数据转换
    }*/

    val data = MLUtils.loadLibSVMFile(sc, filePath)
    val splits = data.randomSplit(Array(0.7, 0.2, 0.1), seed = 11L) //对数据进行分配
    val trainingData = splits(0) //设置训练数据
    val testData = splits(1) //设置测试数据
    val showData = splits(2)

    (trainingData, testData, showData)
  }

}
