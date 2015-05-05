package me.zhongsheng.spark.kafka

import java.util.Properties

import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{TopicMetadataRequest, FetchRequest, PartitionFetchInfo, FetchResponsePartitionData}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndOffset, MessageAndMetadata}
import kafka.serializer.Decoder

import org.slf4j.LoggerFactory

import me.zhongsheng.spark.kafka.serializer.DefaultDecoder


object KafkaStream {
  private val log = LoggerFactory.getLogger(getClass)

  def apply(kafkaProps: Properties): KafkaStream[Array[Byte], Array[Byte]] =
    apply(kafkaProps, new DefaultDecoder, new DefaultDecoder)

  def apply[K, V](kafkaProps: Properties, keyDecoder: Decoder[K], valueDecoder: Decoder[V]): KafkaStream[K, V] =
    new KafkaStream(KafkaConfig(kafkaProps), keyDecoder, valueDecoder)
}

/**
 * Fetch Kafka in a streaming way.
 */
class KafkaStream[K, V] private (config: KafkaConfig, keyDecoder: Decoder[K], valueDecoder: Decoder[V]) {
  import KafkaStream._

  private val brokers = config.metadataBrokerList.split(",").map(KafkaBroker(_))
  private val socketTimeoutMs = config.socketTimeoutMs
  private val socketReceiveBufferBytes = config.socketReceiveBufferBytes
  private val fetchMessageMaxBytes = config.fetchMessageMaxBytes
  private val consumerId = config.consumerId
  private val retries = config.retries
  private val refreshLeaderBackoffMs = config.refreshLeaderBackoffMs

  def fetch(topicAndPartition: TopicAndPartition, offsetFetchInfo: OffsetFetchInfo): Stream[MessageAndMetadata[K, V]] = {
    val OffsetFetchInfo(offsetFrom, offsetTo) = offsetFetchInfo

    def buildMessageAndMetadata(messageAndOffset: MessageAndOffset): MessageAndMetadata[K, V] = MessageAndMetadata(
      topicAndPartition.topic, topicAndPartition.partition,
      messageAndOffset.message, messageAndOffset.offset,
      keyDecoder, valueDecoder
    )

    def findLeader: KafkaBroker = Stream(1 to retries: _*).map { _ => {
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
      val KafkaBroker(leaderHost, leaderPort) = findLeader
      new SimpleConsumer(leaderHost, leaderPort, socketTimeoutMs, socketReceiveBufferBytes, consumerId)
    }

    def doFetch(consumer: SimpleConsumer, offset: Long, retriesLeft: Int): Stream[Seq[MessageAndMetadata[K, V]]] = {
      val FetchResponsePartitionData(errorCode, _, messageSet) = consumer.fetch(FetchRequest(
        requestInfo = Map(topicAndPartition -> PartitionFetchInfo(offset, fetchMessageMaxBytes))
      )).data(topicAndPartition)

      (errorCode, retriesLeft) match {
        case (ErrorMapping.NoError, _) => {
          val messageAndOffsets = messageSet.toArray
          messageAndOffsets.length match {
            case 0 => {
              consumer.close()
              if (offset >= offsetTo) {
                Stream.empty
              } else {
                log.error(s"error fetch offset ${offset} at ${consumer.host}:${consumer.port}")
                throw ErrorMapping.exceptionFor(ErrorMapping.OffsetOutOfRangeCode)
              }
            }
            case _ => {
              val lastMessageAndOffset = messageAndOffsets.last
              if (lastMessageAndOffset.offset >= offsetTo) {
                consumer.close()
                messageAndOffsets.filter(_.offset <= offsetTo).map(buildMessageAndMetadata) #:: Stream.empty
              } else {
                messageAndOffsets.map(buildMessageAndMetadata) #:: doFetch(consumer, lastMessageAndOffset.nextOffset, retries)
              }
            }
          }
        }
        case (_, 0) => {
          consumer.close()
          log.error(s"error fetch offset ${offset} at ${consumer.host}:${consumer.port}")
          throw ErrorMapping.exceptionFor(errorCode)
        }
        case (_, _) => {
          consumer.close()
          Thread.sleep(refreshLeaderBackoffMs)
          doFetch(buildConsumer(findLeader), offset, retriesLeft - 1)
        }
      }
    }

    doFetch(buildConsumer(findLeader), offsetFrom, retries).flatten.dropWhile(_.offset < offsetFrom)
  }

}

