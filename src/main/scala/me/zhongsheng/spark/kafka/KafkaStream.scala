package me.zhongsheng.spark.kafka

import java.util.Properties

import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{FetchRequest, PartitionFetchInfo, FetchResponsePartitionData}
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

  private val fetchMessageMaxBytes = config.fetchMessageMaxBytes
  private val retries = config.retries
  private val refreshLeaderBackoffMs = config.refreshLeaderBackoffMs

  private val kafkaHelper = new KafkaHelper(config)
  import kafkaHelper.{findLeader, buildConsumer}

  def fetch(topicAndPartition: TopicAndPartition, offsetFetchInfo: OffsetFetchInfo): Stream[MessageAndMetadata[K, V]] = {
    val OffsetFetchInfo(offsetFrom, offsetTo) = offsetFetchInfo

    def buildMessageAndMetadata(messageAndOffset: MessageAndOffset): MessageAndMetadata[K, V] = MessageAndMetadata(
      topicAndPartition.topic, topicAndPartition.partition,
      messageAndOffset.message, messageAndOffset.offset,
      keyDecoder, valueDecoder
    )

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
          doFetch(buildConsumer(findLeader(topicAndPartition)), offset, retriesLeft - 1)
        }
      }
    }

    doFetch(buildConsumer(findLeader(topicAndPartition)), offsetFrom, retries).flatten.dropWhile(_.offset < offsetFrom)
  }

}

