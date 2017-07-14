package cn.net.yunshan.nbad.DDoS

/**
  * Created by ShellMount on 2017/7/13.
  */
class ClassifyAlgorithmFactory extends AbstractFactory{
  override def getClassifier(algorithmName: String): ClassifyAlgorithm = {
    if (algorithmName.equalsIgnoreCase("NativeBayes")) new ClassifyModelNaiveBayes()
    else if (algorithmName.equalsIgnoreCase("LogisticRegression")) new ClassifyModelLogisticRegression()
    else new ClassifyModelSupportVectorMachine()
  }
}
