package MyReceiver

class EntropyVector {
  var sum = 0.0
  var variance = 0.0
  var avg = 0.0
  var stddev = 0.0

  override def toString: String = s"$sum,$variance,$avg,$stddev"

  def setValues(sum: Double, variance: Double, avg: Double, stddev: Double): Unit = {
    this.sum = sum
    this.variance = variance
    this.avg = avg
    this.stddev = stddev
  }
}
