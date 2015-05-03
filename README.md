Spark-Kafka-RDD
===================


Spark-Kafka-RDD is a scala library to make Kafka to be a data source of the Spark platform. Note that Spark-Kafka-RDD fetches given offset ranges from Kafka topics and partitions as a single RDD returned to the Spark driver program, not producing a Spark DStream which Spark streaming framework does. 



Features
-------------------

Spark-Kafka-RDD has several useful features.

- KafkaOffsetSeeker simplifies Kafka offset seeking.
- Given a list of brokers, KafkaRDD automatically finds the leader of a topic and partition, and handles when the leader changes.
- KafkaRDD automatically retries when fetching messages failed.
- KafkaRDD automatically split large offset ranges into small ones according to user's configuration, for better parallelism and load balance among all executors.



Usage
-------------

```
import org.apache.spark.SparkContext
import me.zhongsheng.spark.kafka.{KafkaOffsetSeeker, OffsetFetchInfo}
import me.zhongsheng.spark.kafka.rdd.RDD

object Main {
  
  val conf = ...
  val kafkaProps = ...
  val topicAndPartition = ...
  val timefrom = ...
  
  def main(args: Array[String]) {
    val sparkContext = new SparkContext(conf)
    
    val seeker = new KafkaOffsetSeeker(kafkaProps)
    val from = seeker.possibleOffsetBefore(topicAndPartition, timefrom)
    val to = seeker.latestOffset(topicAndPartition)

    val fetchInfo = Map(topicAndPartition -> Seq(
      OffsetFetchInfo(from, to)
    ))
    val rdd = KafkaRDD(sparkContext, kafkaProps, fetchInfo).persist(...)

  ...
  }
}
```



Tuning
-------------------


- Try to avoid fetching a large number of small range of offsets. It is inefficient because it can't fully utilize the network bandwidth. It is a good idea to fetch several pieces of large range of offsets and filter it. Of course, it depends.
- Any offset ranges larger than "fetch.message.max.count" will be automatically splitted into small pieces so they can be fetched simultaneously at different Spark executors. The default value of "fetch.message.max.count" is quite large. Give it a smaller value to achieve better parallelism.
- Adjust "spark.locality.wait" to prevent Spark from fetching the same KafkaRDD partition on another Spark executor to calculate as the executor holding this partition is busy for several seconds.
- Persist KafkaRDD if needed. But never persist one piece of messages twice! For example, if rdd1 and rdd2 are both persisted, don't persist rdd1.union(rdd2)!


