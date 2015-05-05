package me.zhongsheng.spark.kafka

import java.util.Properties

import kafka.common.TopicAndPartition

import org.scalatest.Suite


class KafkaStreamTest extends Suite {

  private val kafkaProps = new Properties
  kafkaProps.put("metadata.broker.list", "192.168.44.206:9092,192.168.44.210:9092")
  kafkaProps.put("socket.receive.buffer.bytes", s"${1024 * 1024}")
  kafkaProps.put("fetch.message.max.bytes", s"${1024 * 1024}")

  private val topicAndPartition = TopicAndPartition("test", 0)

  def testFetchAsStream() {
    val seeker = new KafkaOffsetSeeker(kafkaProps)
    val kafkaStream = KafkaStream(kafkaProps)

    val latestOffset = seeker.latestOffset(topicAndPartition)
    val offsetFetchInfo = OffsetFetchInfo(latestOffset - 10000, latestOffset)

    val iterator = kafkaStream.fetch(topicAndPartition, offsetFetchInfo).iterator
    var i = 0
    while (iterator.hasNext) {
      iterator.next()
      println(i)
      i += 1
    }
  }

}

