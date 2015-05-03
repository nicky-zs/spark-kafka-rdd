package me.zhongsheng.spark.kafka


/**
 * Offset range, for PartitionFetchInfo only represents start with fetch buffer size!
 * Both 'from' and 'to' are inclusive.
 */
case class OffsetFetchInfo(from: Long, to: Long) {
  require(0 <= from && from <= to)

  override def toString = s"Offset[${from}, ${to}]"
}

