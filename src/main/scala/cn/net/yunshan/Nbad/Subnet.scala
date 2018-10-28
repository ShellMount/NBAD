package cn.net.yunshan.Nbad

class Subnet(subnets: Array[String]) extends Serializable {

  var netRanges: Array[(Long, Long)] = Array()
  subnets2Ranges(subnets)

  def notBelongInternal(ip: String): Boolean = {
    if (ip == null) return true
    var ipLong = ip2Long(ip)
    for ((bottom, ceiling) <- netRanges) {
      if (bottom < ipLong && ipLong < ceiling) {
        return false
      }
    }
    return true
  }

  def ip2Long(ip: String): Long = {
    ip.split("\\.").foldLeft(0L) {
      (sum, v) => (sum << 8) + v.toLong
    }
  }

  def subnets2Ranges(subnets: Array[String]): Unit = {
    for (net <- subnets) {
      if (net != "") {
        netRanges = netRanges :+ subnet2Range(net)
      }
    }
  }

  def subnet2Range(subnet: String): (Long, Long) = {
    val net = subnet.split("/")
    val bottom = ip2Long(net(0)) & (0xFFFFFFFF << (32 - net(1).toInt))
    val ceiling = bottom | ~(0xFFFFFFFF << (32 - net(1).toInt))
    (bottom, ceiling)
  }
}
