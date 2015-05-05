package me.zhongsheng.spark.kafka.rdd

import java.util.Properties

import org.apache.spark.{SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD

import kafka.common.{TopicAndPartition, ErrorMapping}
import kafka.api.{TopicMetadataRequest, FetchRequest, PartitionFetchInfo, FetchResponsePartitionData}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset

import org.slf4j.LoggerFactory

import me.zhongsheng.spark.kafka.{KafkaConfig, KafkaBroker, OffsetFetchInfo, KafkaStream}


object KafkaRDD {
  private val log = LoggerFactory.getLogger(getClass)

  def apply(sc: SparkContext,
    kafkaProps: Properties,
    fetchInfo: Map[TopicAndPartition, Seq[OffsetFetchInfo]],
    fetchMessageMaxCount: Int = 1024 * 1024 * 1024) = new KafkaRDD(sc, kafkaProps, fetchInfo, fetchMessageMaxCount)
}

/**
 * An RDD to fetch messages from a kafka cluster.
 * OffsetFetchInfo MUST be correct to avoid exceptions.
 * 
 * @param fetchMessageMaxCount a value used to cut large range of offsets into small pieces
 */
class KafkaRDD private (
  _sc: SparkContext,
  kafkaProps: Properties,
  fetchInfo: Map[TopicAndPartition, Seq[OffsetFetchInfo]],
  fetchMessageMaxCount: Int
)
extends RDD[MessageAndOffset](_sc, Nil) {
  import KafkaRDD._

  override def compute(split: Partition, context: TaskContext): Iterator[MessageAndOffset] = {
    if (context.attemptNumber > 1) {
      log.warn(s"Attempt ${context.attemptNumber} times for fetching ${split}")
    }

    val taskStartTime = System.currentTimeMillis
    context.addTaskCompletionListener(_ => {
      val used = (System.currentTimeMillis - taskStartTime) / 1000.0
      if (used > 300.0) { log.warn(s"Fetched ${split} in quite a long time! (${used}s)") }
    })

    val topicAndPartition = split.asInstanceOf[KafkaRDDPartition].topicAndPartition
    val offsetFetchInfo = split.asInstanceOf[KafkaRDDPartition].offsetFetchInfo

    KafkaStream(kafkaProps).fetch(topicAndPartition, offsetFetchInfo).iterator
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


