package me.zhongsheng.spark.kafka

import java.util.Properties

import kafka.utils.VerifiableProperties


/**
 * Config for Kafka.
 */
class KafkaConfig private (props: VerifiableProperties) {
  import KafkaConfig._

  val metadataBrokerList = props.getString("metadata.broker.list")

  val consumerId = props.getString("consumer.id", DefaultConsumerId)

  val socketTimeoutMs = props.getInt("socket.timeout.ms", DefaultSocketTimeoutMs)
  require(socketTimeoutMs > 0, "socket.timeout.ms must be greater than 0")

  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", DefaultSocketReceiveBufferBytes)
  require(socketReceiveBufferBytes >= 1024 * 1024, "socket.receive.buffer.bytes must be greater than or equal to ${1024 * 1024}")

  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", DefaultFetchMessageMaxBytes)
  require(fetchMessageMaxBytes >= 1024 * 1024, "fetch.message.max.bytes must be greate than or equal to ${1024 * 1024}")

  val fetchMessageMaxCount = props.getInt("fetch.message.max.count", DefaultFetchMessageMaxCount)
  require(fetchMessageMaxCount <= 1024 * 1024 * 1024, "fetch.message.max.count must be less than or equal to ${1024 * 1024 * 1024}")

  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", DefaultRefreshLeaderBackoffMs)
  require(refreshLeaderBackoffMs > 0, "refresh.leader.backoff.ms must be greater than 0")

  val retries = props.getInt("retries", DefaultRetries)
  require(retries >= 0, "retries must be greater than or equal to 0")

}

object KafkaConfig {
  def apply(props: Properties) = new KafkaConfig(new VerifiableProperties(props))

  val DefaultConsumerId = ""
  val DefaultSocketTimeoutMs = 30 * 1000
  val DefaultSocketReceiveBufferBytes = 64 * 1024
  val DefaultFetchMessageMaxBytes = 1024 * 1024
  val DefaultFetchMessageMaxCount = 1024 * 1024 * 1024
  val DefaultRefreshLeaderBackoffMs = 200
  val DefaultRetries = 3
}

