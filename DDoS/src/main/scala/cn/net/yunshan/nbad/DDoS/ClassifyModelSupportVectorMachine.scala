package cn.net.yunshan.nbad.DDoS

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by ShellMount on 2017/7/13.
  */

class ClassifyModelSupportVectorMachine extends ClassifyAlgorithm with Serializable{
  val modelName = "支持向量机"
  var model: SVMModel = null
  var predictionAndLabel:  RDD[(Double, Double)] = null
  var testDataLen: Long = 0
  var accuracyCount: Double = 0.0

  /**
    * 训练模型
    * @param trainingData
    * @return
    */
  override def train(trainingData: RDD[LabeledPoint]) = {
    /**
      * 训练模型参数调整记录: 无
      */
    model = SVMWithSGD.train(trainingData, Config.numIterations)
  }

  override def predict(testData: RDD[LabeledPoint]) {
    testDataLen = testData.count()
    predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
  }

  /**
    * 准确度: 模型标注正确的数量
    */
  override def accuracy {
    accuracyCount = 1.0 * predictionAndLabel.filter(label => label._1 == label._2).count()
  }

  /**
    * 模型评估
    */
  override def evaluate() {
    println("模型名称: " + modelName + "\n" + "==" * 30)

    val metrics = new MulticlassMetrics(predictionAndLabel)			//创建验证类
    val precision = metrics.precision								            //计算验证值
    println("Precision = " + precision)							            //打印验证值


    //showData.foreach((label, feature)_ => println(_.features))
    val checkVector = Vectors.dense(Array(0, 0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 709.0))
    println("检验一个随机数组结果：" + model.predict(checkVector))

    val abuseIp = Vectors.dense(Array(0, 0, 1.0, 1, 1, 193, 193, 193, 197632))
    println("这个恶意IP在模型中检验出来的结果是：" + model.predict(abuseIp))

    val abuseIp2 = Vectors.dense(Array(0, 0, 1.0, 1, 1, 93, 13, 19, 632))
    println("这个恶意IP的变种能否检验出来？：" + model.predict(abuseIp2))


    // 另一个维度的考虑
    val Some(ddos) = Config.attackClassID.get("DDoS")
    val Some(nonDDoS) = Config.attackClassID.get("NonDDoS")
    val ddosDoubleID = ddos.toDouble
    val nonDDoSDoubleID = nonDDoS.toDouble

    val predictAbuseIpRDD = predictionAndLabel.filter(label => label._1.toDouble == ddosDoubleID)

    val predictAbuseIpCorrectRDD = predictionAndLabel.filter(label => label._1 == label._2 && label._1.toDouble == ddosDoubleID)

    val labeledAbuseIpRDD = predictionAndLabel.filter(label => label._2.toDouble == ddosDoubleID)

    val unknowPredictAsDDoSRDD = predictionAndLabel.filter(label => label._1 == ddosDoubleID && label._2 == nonDDoSDoubleID)


    println("==" * 30)
    printf("准确度: %.4f%%\n", accuracyCount / testDataLen * 100)
    println("测试集总量：" + testDataLen)
    println("测试集中人工标注的 DDoS 数量 ： " + labeledAbuseIpRDD.count())
    println("测试集中模型检测到 恶意IP 总数为： " + predictAbuseIpRDD.count)
    println("测试集中模型检测到 恶意IP 正确的总数为： " + predictAbuseIpCorrectRDD.count())
    println("测试集中 非DDoS 检测为 DDoS 的量：" + unknowPredictAsDDoSRDD.count())
    println("测试集中使用模型检测失败数量：" + (testDataLen - accuracyCount))

    println("成功的判断像这样：")
    predictAbuseIpCorrectRDD.foreach(println)
  }



}
