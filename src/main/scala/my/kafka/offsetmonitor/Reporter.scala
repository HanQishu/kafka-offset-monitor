package my.kafka.offsetmonitor

import kafka.coordinator.group.GroupTopicPartition
import my.kafka.offsetmonitor.bean.OffsetInfo
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
  * Created by huangxingbiao on 2019/1/9.
  */
trait Reporter extends Initializable {

  var monitor: MonitorCenter = _

  def offset(): Map[TopicPartition, mutable.Map[String, OffsetInfo]] = monitor.offsetInfoMap.toMap

  def offset(group: String): Map[TopicPartition, OffsetInfo] = {
    monitor.offsetInfoMap.flatMap {
      case (tp, groupInfo) => groupInfo.filter(i => group.eq(i._1)).map(i => (tp, i._2))
    }.toMap
  }

  def offset(group: String, topic: String): Map[Int, OffsetInfo] =
    offset(group).filter(i => topic.eq(i._1.topic())).map {
      case (tp, offsetInfo) => (tp.partition(), offsetInfo)
    }

  def groupTopicPartition(): Set[GroupTopicPartition] = {
    monitor.offsetInfoMap.flatMap {
      case (tp, groupInfo) => groupInfo.keys.map(GroupTopicPartition(_, tp))
    }.toSet
  }

  def groups(): Set[String] = groupTopicPartition().map(_.group)

  def groups(topic: String): Set[String] =
    groupTopicPartition().filter(i => topic.eq(i.topicPartition.topic())).map(_.group)

  def topics(): Set[String] = groupTopicPartition().map(_.topicPartition.topic)

  def topics(group: String): Set[String] =
    groupTopicPartition().filter(i => group.eq(i.group)).map(_.topicPartition.topic)

  def report(infoes: IndexedSeq[OffsetInfo]): Unit

}
