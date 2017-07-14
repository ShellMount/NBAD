package cn.net.yunshan.nbad.DDoS

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ShellMount on 2017/7/4.
  */
object MainDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(Config.master).setAppName(Config.appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel(Config.logLevel)

    /**
      * 历史数据获取
      */
    val filePath = Thread.currentThread().getContextClassLoader.getResource(Config.featureFileName).getPath
    val (trainingData, testData, showData) = new DataPretreatment().getSvm(sc, filePath)

    println("数据准备完成, 模型启动...")
    println("==" * 30)

    /**
      * 模型生成
      * 分类器工厂支持 NativeBayes, LogisticRegression, SVM
      */
    val classifyFactory = new FactoryProducer().getFactory(Config.factoryNameClassify)
    val classifier = classifyFactory.getClassifier(Config.algorithmModel)
    classifier.train(trainingData)

    /**
      * 使用模型
      */
    classifier.predict(testData)

    classifier.accuracy()

    classifier.evaluate()
  }
}
