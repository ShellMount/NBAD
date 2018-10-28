package cn.net.yunshan.Nbad

object StatusEnum extends Enumeration {
  type SourceType = Value
  val normal, suspicious, malicious = Value
}
