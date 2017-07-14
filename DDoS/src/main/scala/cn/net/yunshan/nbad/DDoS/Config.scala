package cn.net.yunshan.nbad.DDoS

import scala.collection.mutable

/**
  * Created by ShellMount on 2017/7/13.
  */
object Config {

  /**
    * 基本设置
    */
  val appName = "DDoSMLModelDemo"
  val master = "local"
  val logLevel = "ERROR"

  // 攻击类型
  val attackClassID = mutable.Map(
    "safe" -> 10,
    "botnet" -> 101,
    "brute_force" -> 102,
    "DDoS" -> 1,
    "malicious" -> 104,
    "scanner" -> 105,
    "spam" -> 106,
    "unknow" -> 0,
    "NonDDoS" -> 0
  )

  // 特征文件名称
  // 数据文件要放到 resources 文件夹下
  val featureFileName = "labeledFea.svm"

  // 工厂类获取
  val factoryNameClassify = "CLASSIFY"


  /**
    * 算法参数相关
    */
  // 模型算法
  // 支持算法：
  // NativeBayes, LogisticRegression, SVM
  val algorithmModel = "SVM"

  // 算法迭代次数
  val numIterations = 100



  // for test
  def main(args: Array[String]): Unit = {

    val Some(v) = attackClassID.get("DDoS")

    println (v.toDouble)
  }
}
