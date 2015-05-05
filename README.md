Spark-Kafka-RDD
===================


Spark-Kafka-RDD is a scala library to make Kafka to be a data source of the Spark platform. Note that Spark-Kafka-RDD fetches given offset ranges from Kafka topics and partitions as a single RDD (```KafkaRDD```) returned to the Spark driver program, not producing a Spark ```DStream``` which Spark streaming framework does. 



Features
-------------------

Spark-Kafka-RDD has several useful features.

- ```KafkaOffsetSeeker``` simplifies Kafka offset seeking.
- Given a list of brokers, ```KafkaRDD``` automatically finds the leader of a topic and partition, and handles when the leader changes.
- ```KafkaRDD``` automatically retries when fetching messages failed.
- ```KafkaRDD``` automatically split large offset ranges into small ones according to user's configuration, for better parallelism and load balance among all executors.



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


- Try to avoid fetching a large number of small range of offsets. It is inefficient because it can't fully utilize the network bandwidth. It is a good idea to fetch several pieces of large range of offsets and filter it. Of course, this isn't always true. It depends.
- The value of "```fetch.message.max.bytes```" is the number of bytes ```KafkaRDD``` will fetch at one time. If this value is smaller than the size of your message, ```KafkaRDD``` won't work because it will always fetch 0 messages. Continuously fetching data for hundreds or thousands of milliseconds at one time is a good idea.
- Any offset ranges larger than "```fetch.message.max.count```" will be automatically splitted into small pieces so they can be fetched simultaneously at different Spark executors. The default value of "```fetch.message.max.count```" is quite large. Give it a smaller value to achieve better parallelism.
- The value of "```fetch.message.max.count```" and the value of "```fetch.message.max.bytes```" should match. According to the size of each message, a large "```fetch.message.max.bytes```" with a small "```fetch.message.max.count```" will cause ```KafkaRDD``` to drop a lot of excess data; a large "```fetch.message.max.count```" with a small "```fetch.message.max.bytes```" will cause ```KafkaRDD``` to fetch a lot of different data segments on only one Spark executor serially.
- Adjust "```spark.locality.wait```" to prevent Spark from fetching the same ```KafkaRDD``` partition on another Spark executor to calculate as the executor holding this partition is busy for several seconds.
- Persist ```KafkaRDD``` if needed, but never persist one piece of messages twice. For example, if ```rdd1``` and ```rdd2``` are both persisted, don't persist ```rdd1.union(rdd2)``` again. Keeping doing that in a loop or tail recursion will soon exhaust the memory.
- If your program is constructed in a streaming style, i.e. fetching backlog messages first and then continously append new coming real time messages. In this case, ```RDD.union``` seems to be a good solution for that. However, operations on an ```RDD``` with such deeply recursived dependencies would probably cause a ```StackOverflowError```. Neither enlarging the ```-Xss``` option of JVM nor calling ```RDD.coalesce(numPartition, shuffle = true)``` can solve it ultimately. One solution is to remove all the ```RDD```'s dependencies by using ```checkpoint```. Another solution is maintaining the ```RDD``` flow as a collection manually, only uniting them as needed.


