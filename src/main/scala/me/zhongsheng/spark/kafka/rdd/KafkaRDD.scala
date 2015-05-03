package me.zhongsheng.spark.kafka.rdd

import java.util.Properties

import org.apache.spark.{SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD

import kafka.common.{TopicAndPartition, ErrorMapping}
import kafka.api.{TopicMetadataRequest, FetchRequest, PartitionFetchInfo, FetchResponsePartitionData}
import kafka.consumer.SimpleConsumer
import kafka.message.{Message, MessageAndOffset}

import org.slf4j.LoggerFactory

import me.zhongsheng.spark.kafka.{KafkaConfig, KafkaBroker, OffsetFetchInfo}


object KafkaRDD {
  private val log = LoggerFactory.getLogger(getClass)

  def apply(sc: SparkContext, props: Properties, fetchInfo: Map[TopicAndPartition, Seq[OffsetFetchInfo]]) =
    new KafkaRDD(sc, KafkaConfig(props), fetchInfo)
}

/**
 * An RDD to fetch messages from a kafka cluster.
 * OffsetFetchInfo MUST be correct to avoid exceptions.
 */
class KafkaRDD private (
  _sc: SparkContext,
  config: KafkaConfig,
  fetchInfo: Map[TopicAndPartition, Seq[OffsetFetchInfo]]
)
extends RDD[Message](_sc, Nil) {
  import KafkaRDD._

  private val brokers = config.metadataBrokerList.split(",").map(KafkaBroker(_))
  private val socketTimeoutMs = config.socketTimeoutMs
  private val socketReceiveBufferBytes = config.socketReceiveBufferBytes
  private val fetchMessageMaxBytes = config.fetchMessageMaxBytes
  private val fetchMessageMaxCount = config.fetchMessageMaxCount
  private val consumerId = config.consumerId
  private val retries = config.retries
  private val refreshLeaderBackoffMs = config.refreshLeaderBackoffMs

  override def compute(split: Partition, context: TaskContext): Iterator[Message] = {
    if (context.attemptNumber > 1) { log.warn(s"Attempt ${context.attemptNumber} times for fetching ${split}") }

    val taskStartTime = System.currentTimeMillis
    context.addTaskCompletionListener(_ => {
      val used = (System.currentTimeMillis - taskStartTime) / 1000.0
      if (used > 300.0) { log.warn(s"Fetched ${split} in quite a long time! (${used}s)") }
    })

    val topicAndPartition = split.asInstanceOf[KafkaRDDPartition].topicAndPartition
    val OffsetFetchInfo(offsetFrom, offsetTo) = split.asInstanceOf[KafkaRDDPartition].offsetFetchInfo

    def makeConsumer(broker: KafkaBroker) = {
      val KafkaBroker(leaderHost, leaderPort) = findLeader(topicAndPartition)
      val consumer = new SimpleConsumer(leaderHost, leaderPort, socketTimeoutMs, socketReceiveBufferBytes, consumerId)
      context.addTaskCompletionListener(_ => consumer.close())
      consumer
    }

    def fetch(consumer: SimpleConsumer, offset: Long, retriesLeft: Int): Stream[MessageAndOffset] = {
      val FetchResponsePartitionData(errorCode, _, messageSet) = consumer.fetch(FetchRequest(
        requestInfo = Map(topicAndPartition -> PartitionFetchInfo(offset, fetchMessageMaxBytes))
      )).data(topicAndPartition)

      (errorCode, retriesLeft) match {
        case (ErrorMapping.NoError, _) => {
          val messageAndOffsets = messageSet.toArray
          messageAndOffsets.length match {
            case 0 => if (offset >= offsetTo) messageAndOffsets ++: (Stream.empty) else {
              log.error(s"error fetch offset ${offset} at ${consumer.host}:${consumer.port} at attempt ${context.attemptNumber}")
              throw ErrorMapping.exceptionFor(ErrorMapping.OffsetOutOfRangeCode)
            }
            case _ => {
              val lastOffset = messageAndOffsets.last.offset
              messageAndOffsets ++: (if (lastOffset >= offsetTo) Stream.empty else fetch(consumer, lastOffset + 1, retries))
            }
          }
        }
        case (_, 0) => {
          log.error(s"error fetch offset ${offset} at ${consumer.host}:${consumer.port} at attempt ${context.attemptNumber}")
          throw ErrorMapping.exceptionFor(errorCode)
        }
        case (_, _) => {
          consumer.close()
          Thread.sleep(refreshLeaderBackoffMs)
          fetch(makeConsumer(findLeader(topicAndPartition)), offset, retriesLeft - 1)
        }
      }
    }

    fetch(makeConsumer(findLeader(topicAndPartition)), offsetFrom, retries)
      .filter(_.offset >= offsetFrom).takeWhile(_.offset <= offsetTo).map(_.message).iterator
  }

  protected override val getPartitions: Array[Partition] = {
    /* to cut a big OffsetFetchInfo into small ones */
    def slice(offsetFetchInfo: OffsetFetchInfo, maxSizeForSlice: Int): Seq[OffsetFetchInfo] = {
      val OffsetFetchInfo(from, to) = offsetFetchInfo
      (1 + to - from).ensuring(_ < Int.MaxValue).toInt match {
        case totalSize if totalSize > maxSizeForSlice => {
          val buckets = (totalSize + maxSizeForSlice - 1) / maxSizeForSlice
          val (size, rest) = (totalSize / buckets, totalSize % buckets)
          val sliceSizes = (1 to buckets).map(x => if (x <= rest) 1 else 0).map(_ + size)
          val grads = sliceSizes.inits.map(_.sum).map(_ + from - 1).toSeq.reverse
          grads.sliding(2).map(slice => OffsetFetchInfo(slice(0) + 1, slice(1))).toSeq
        }
        case _ => Seq(offsetFetchInfo)
      }
    }

    fetchInfo.map {
      case (topicAndPartition, partitionFetchInfos) =>
        partitionFetchInfos.flatMap(slice(_, fetchMessageMaxCount)).map { (topicAndPartition, _) }
    }.flatten.zipWithIndex.map {
      case ((topicAndPartition, offsetFetchInfo), index) => new KafkaRDDPartition(id, index, topicAndPartition, offsetFetchInfo)
    }.toArray
  }

  private def findLeader(topicAndPartition: TopicAndPartition): KafkaBroker =
    Stream(1 to retries: _*).map { _ => {
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
}


/**
 * A spark Partition for Kafka messages of some topic.
 * The topic partition as well as the offset range can be specified.
 * The topic itself can only be specified in KafkaRDD.
 */
private[spark] class KafkaRDDPartition(
  rddId: Int,
  override val index: Int,
  val topicAndPartition: TopicAndPartition,
  val offsetFetchInfo: OffsetFetchInfo) extends Partition {

  override def hashCode: Int = 41 * (41 + rddId) + index
  override def toString: String = "{rddId: %d, index: %d, topic: %s, partition: %d, offset: %s}"
    .format(rddId, index, topicAndPartition.topic, topicAndPartition.partition, offsetFetchInfo)
}


