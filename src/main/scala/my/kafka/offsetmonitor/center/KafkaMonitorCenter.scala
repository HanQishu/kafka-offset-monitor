package my.kafka.offsetmonitor.center

import java.nio.ByteBuffer
import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.{GroupMetadataManager, GroupTopicPartition, OffsetKey}
import kafka.utils.Logging
import my.kafka.offsetmonitor.MonitorCenter
import my.kafka.offsetmonitor.bean.OffsetInfo
import my.kafka.offsetmonitor.common.KafkaCreator
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

/**
  * Created by huangxingbiao on 2019/1/15.
  */
class KafkaMonitorCenter extends MonitorCenter with Logging {

  import scala.concurrent.ExecutionContext.Implicits.global

  val interval: Long = 3000L
  val offsetTopic = "__consumer_offsets"
  var bootstrap: String = _

  var admin: AdminClient = _
  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _
  var consumerGroups: mutable.Map[String, KafkaConsumer[Array[Byte], Array[Byte]]] = _

  var exclude_groups: List[Pattern] = List.empty
  var exclude_topics: List[Pattern] = List.empty

  override def initialize(configs: Map[String, _]): Unit = {
    info("initialize kafka monitor center....")
    bootstrap = configs("brokers").toString

    val exclude = configs("exclude").asInstanceOf[java.util.Map[String, java.util.List[String]]]
    if (exclude.containsKey("groups"))
      exclude_groups = exclude.get("groups").asScala.map(_.r.pattern).toList
    if (exclude.containsKey("topics"))
      exclude_topics = exclude.get("topics").asScala.map(_.r.pattern).toList

    consumer = KafkaCreator.consumer(bootstrap, offsetTopic)
    admin = KafkaCreator.admin(bootstrap)
    //    admin.describeTopics()
    consumerGroups = mutable.Map()
  }

  override def monitoring(): Unit = {
    logger.info("Staring Kafka offset topic monitor")
    offsetMonitor() // consume __consumer_offsets topic
  }

  def offsetMonitor(): Unit = {
    Future {
      try {
        while (true) {

          updateConsumed()
          updateNonConsumed()

          TimeUnit.MILLISECONDS.sleep(interval)
        }
      } catch {
        case e: Throwable =>
          fatal(s"$bootstrap's Offset topic listener aborted dur to unexpected exception", e)
          System.exit(1)
      }
    }
  }

  private def updateConsumed(): Unit = {
    try {
      var count = 0
      for (record <- consumer.poll(Duration.ofMillis(5000)).asScala) {
        breakable {
          GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key())) match {
            case offsetKey: OffsetKey if !offsetKey.key.topicPartition.topic().equals(offsetTopic) =>
              if (record.value() == null) break
              if (isExclude(offsetKey.key)) break

              val commitValue: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value()))
              count += 1
              debug("Processed commit message: " + offsetKey + " => " + commitValue)

              val tp = offsetKey.key.topicPartition
              if (!offsetInfoMap.contains(tp)) offsetInfoMap(tp) = mutable.Map()
              offsetInfoMap(tp) += (offsetKey.key.group -> processPartition(offsetKey.key, commitValue))

            case _ => break
          }
        }
      }
      info(s"update $bootstrap's $count message from offset topic")
    } catch {
      case e: RuntimeException =>
        // sometimes offsetMsg.key() || offsetMsg.message() throws NPE
        warn("Failed to process one of the commit message due to exception. The 'bad' message will be skipped", e)
    }
  }

  private def updateNonConsumed(): Unit = {
    val curtime = System.currentTimeMillis()
    var count = 0

    offsetInfoMap.foreach {
      case (tp: TopicPartition, groupInfo: mutable.Map[String, OffsetInfo]) =>

        // if topic is not exists, remove cache
        if (!topicExists(tp.topic())) {
          offsetInfoMap.remove(tp)
          consumerGroups.remove(tp.topic()).get.close()

          info(s"remove consume on $bootstrap's ${tp.topic()}")
        }
        // update logsize from controller
        else {
          var logsize = -1L

          groupInfo.foreach {
            case (group, oldOffsetInfo) =>
              if (oldOffsetInfo.modified + interval < curtime) {
                if (logsize == -1L) logsize = fetchLogSize(tp)

                groupInfo += (group ->
                  OffsetInfo(group, tp.topic(), tp.partition, oldOffsetInfo.offset, logsize, curtime))

                count += 1
              }
          }
        }
    }

    info(s"update $bootstrap's $count message from controller")
  }

  private def processPartition(groupTopicPartition: GroupTopicPartition, offsetAndMetadata: OffsetAndMetadata): OffsetInfo = {
    val group = groupTopicPartition.group
    val topicPartition = groupTopicPartition.topicPartition

    OffsetInfo(group = group,
      topic = topicPartition.topic(),
      partition = topicPartition.partition(),
      offset = offsetAndMetadata.offset,
      logSize = fetchLogSize(topicPartition),
      modified = offsetAndMetadata.commitTimestamp)
  }

  /* important */
  private def fetchLogSize(topicPartition: TopicPartition): Long = {
    val topic = topicPartition.topic()

    try {
      val consumer = consumerGroups.getOrElseUpdate(topic, KafkaCreator.consumer(bootstrap, topic))
      consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition)
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse $bootstrap's partition info. topic: [$topic]", t)
        -1L
    }
  }

  private def topicExists(topic: String): Boolean = {
    Try(admin.describeTopics(Collections.singleton(topic)).values().get(topic).get) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }
  }

  private def isExclude(gtp: GroupTopicPartition): Boolean =
    exclude_groups.exists(_.matcher(gtp.group).matches()) || exclude_topics.exists(_.matcher(gtp.topicPartition.topic()).matches())

}
