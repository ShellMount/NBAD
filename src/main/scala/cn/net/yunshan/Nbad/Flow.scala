package cn.net.yunshan.Nbad

// 定义流的格式

case class Flow (interval: String,
                 flow_id: String,
                 net_src: String,
                 ip_src: String,
                 ip_dst: String,
                 port_src: Int,
                 port_dst: Int,
                 tcp_flags_0: Int,
                 start_time: Long,
                 end_time: Long,
                 duration: Long,
                 proto: Int,
                 total_byte_cnt_0: Long,
                 total_pkt_cnt_0: Long,
                 close_type: Int
                )


//处理数据有效性
case class Flow2 (record: Array[String]) {
  val interval: String = record(0).toString
  val flow_id: String = record(1).toString  // 应该为Long
  val net_src: String = record(2).toString
  val ip_src: String = record(3).toString
  val ip_dst: String = record(4).toString
  val port_src: Int = if (record(5) != "") record(5).toInt else -1
  val port_dst: Int = if (record(6) != "") record(6).toInt else -1
  val tcp_flags_0: Int = if (record(7) != "") record(7).toInt else -1
  val start_time: Long = record(8).toLong
  val end_time: Long = record(9).toLong
  val duration: Long = record(10).toLong
  val proto: Int = record(11).toInt
  val total_byte_cnt_0: Long = record(12).toLong
  val total_pkt_cnt_0: Long = record(13).toLong
  val close_type: Int = record(14).toInt
}

