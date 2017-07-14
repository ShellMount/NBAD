package cn.net.yunshan.nbad.DDoS

/**
  * Created by ShellMount on 2017/7/13.
  */
abstract class AbstractFactory {
  def getClassifier(algorithmName: String): ClassifyAlgorithm

  // 添加其它抽象方法
}
