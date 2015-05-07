package me.zhongsheng.spark.kafka

import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest


class KafkaHelper(config: KafkaConfig) {

  private val brokers = config.metadataBrokerList.split(",").map(KafkaBroker(_))
  private val socketTimeoutMs = config.socketTimeoutMs
  private val socketReceiveBufferBytes = config.socketReceiveBufferBytes
  private val consumerId = config.consumerId
  private val retries = config.retries
  private val refreshLeaderBackoffMs = config.refreshLeaderBackoffMs

  def findLeader(topicAndPartition: TopicAndPartition): KafkaBroker = Stream(1 to retries: _*).map { _ => {
    brokers.toStream.map { broker => {
      val consumer = new SimpleConsumer(broker.host, broker.port, socketTimeoutMs, socketReceiveBufferBytes, consumerId)
      try {
        consumer.send(new TopicMetadataRequest(Seq(topicAndPartition.topic), 0)).topicsMetadata.toStream.flatMap {
          case topicMeta if (topicMeta.errorCode == ErrorMapping.NoError &&
            topicMeta.topic == topicAndPartition.topic) => topicMeta.partitionsMetadata
        }.map {
          case partitionMeta if (partitionMeta.errorCode == ErrorMapping.NoError &&
            partitionMeta.partitionId == topicAndPartition.partition) => partitionMeta.leader
        } collectFirst {
          case Some(broker) => KafkaBroker(broker.host, broker.port)
        }   
      } catch { case _: Throwable => None } finally { consumer.close() }
    }} collectFirst { case Some(broker) => broker }
  }} filter {
    case Some(_) => true
    case None => Thread.sleep(refreshLeaderBackoffMs); false
  } collectFirst { case Some(broker) => broker } match {
    case Some(broker) => broker
    case None => throw new Exception("Find leader failed!")
  }

  def buildConsumer(broker: KafkaBroker) = {
    val KafkaBroker(leaderHost, leaderPort) = broker
    new SimpleConsumer(leaderHost, leaderPort, socketTimeoutMs, socketReceiveBufferBytes, consumerId)
  }

}

