package MyReceiver

// 定义流的格式

case class Flow (flow_id: String,
                 net_src: String,
                 ip_src: String,
                 ip_dst: String,
                 port_src: Int,
                 port_dst: Int,
                 tcp_flags_0: Int,
                 start_time: Long,
                 end_time: Long,
                 duration: Int,
                 proto: Int,
                 total_byte_cnt_0: Long,
                 total_pkt_cnt_0: Long
                )


case class Flow2 (record: Array[String]) {
  val flow_id: String = record(0).toString  // 应该为Long
  val net_src: String = record(1).toString
  val ip_src: String = record(2).toString
  val ip_dst: String = record(3).toString
  val port_src: Int = record(4).toInt
  val port_dst: Int = record(5).toInt
  val tcp_flags_0: Int = record(6).toInt
  val start_time: Long = record(7).toLong
  val end_time: Long = record(8).toLong
  val duration: Int = record(9).toInt
  val proto: Int = record(10).toInt
  val total_byte_cnt_0: Long = record(11).toLong
  val total_pkt_cnt_0: Long = record(12).toLong
}

