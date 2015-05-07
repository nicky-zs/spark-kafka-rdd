package me.zhongsheng.spark.kafka

import java.util.Properties

import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, PartitionOffsetsResponse}


class KafkaOffsetSeeker(props: Properties) {

  private val config = KafkaConfig(props)

  private val kafkaHelper = new KafkaHelper(config)
  import kafkaHelper.{findLeader, buildConsumer}

  private val earliest = -2
  private val latest = -1

  def possibleOffsetBefore(topicAndPartition: TopicAndPartition, timeMillis: Long): Long = {
    Stream.range(0, config.retries).map { _ => {
      val request = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(timeMillis, 1)))
      val leader = buildConsumer(findLeader(topicAndPartition)) 
      val resp = leader.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition)
      leader.close()
      resp match {
        case PartitionOffsetsResponse(ErrorMapping.NoError, Seq(offset, _*)) => Some(offset: Long)
        case _ => Thread.sleep(config.refreshLeaderBackoffMs); None
      }
    }} collectFirst { case Some(offset) => offset } match {
      case Some(offset) => offset
      case None => throw new Exception("Fetch offset failed!")
    }
  }

  def earliestOffset(topicAndPartition: TopicAndPartition): Long = possibleOffsetBefore(topicAndPartition, earliest)
  def latestOffset(topicAndPartition: TopicAndPartition): Long = possibleOffsetBefore(topicAndPartition, latest)

}

