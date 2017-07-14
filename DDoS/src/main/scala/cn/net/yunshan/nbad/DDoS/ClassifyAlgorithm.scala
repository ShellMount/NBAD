package cn.net.yunshan.nbad.DDoS

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by ShellMount on 2017/7/13.
  */
trait ClassifyAlgorithm {
  def train(trainingData: RDD[LabeledPoint])

  def predict(testData: RDD[LabeledPoint])

  def evaluate()

  def accuracy()
}
