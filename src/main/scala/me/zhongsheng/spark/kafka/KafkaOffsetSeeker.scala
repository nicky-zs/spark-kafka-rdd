package me.zhongsheng.spark.kafka

import java.util.Properties

import kafka.consumer.SimpleConsumer
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}


class KafkaOffsetSeeker(props: Properties) {

  val config = KafkaConfig(props)
  val broker = config.metadataBrokerList.split(",").map(KafkaBroker(_)).head

  val consumer = new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.consumerId)

  def possibleOffsetBefore(topicAndPartition: TopicAndPartition, timeMillis: Long): Long = {
    val requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(timeMillis, 1))
    val resp = consumer.getOffsetsBefore(OffsetRequest(requestInfo = requestInfo)).partitionErrorAndOffsets(topicAndPartition)
    ErrorMapping.maybeThrowException(resp.error)
    resp.offsets.head
  }

  def earliestOffset(topicAndPartition: TopicAndPartition): Long = consumer.earliestOrLatestOffset(topicAndPartition, -2, 0)
  def latestOffset(topicAndPartition: TopicAndPartition): Long = consumer.earliestOrLatestOffset(topicAndPartition, -1, 0)

  def close() = consumer.close()

}

