package cn.net.yunshan.nbad.DDoS

/**
  * Created by ShellMount on 2017/7/13.
  */
class FactoryProducer {
  def getFactory(choice: String): AbstractFactory = {
    // 分类算法工厂
    if (choice.equalsIgnoreCase("CLASSIFY")){
      new ClassifyAlgorithmFactory()
    } else {
      // 增加其它工厂
      // 最后默认返回
      new ClassifyAlgorithmFactory()
    }
  }

}
