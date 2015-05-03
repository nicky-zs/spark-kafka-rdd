package me.zhongsheng.spark.kafka


/**
 * kafka.cluster.Broker is not suitable everywhere for it requires an id
 */
case class KafkaBroker(host: String, port: Int)

object KafkaBroker {
  def apply(addr: String): KafkaBroker = addr.split(":") match {
    case Array(host, port) => KafkaBroker(host, port.toInt)
    case Array(host) => KafkaBroker(host, 9092)
    case _ => throw new IllegalArgumentException(addr)
  }
}


