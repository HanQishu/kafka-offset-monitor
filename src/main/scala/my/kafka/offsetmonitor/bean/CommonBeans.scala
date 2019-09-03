package my.kafka.offsetmonitor.bean

import org.apache.kafka.common.Node

import scala.collection.Seq

case class GroupAndTopic(group: String, topic: String)

case class NodeX(name: String, children: Seq[NodeX] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class TopicAndConsumersDetails(active: Seq[KafkaInfo], inactive: Seq[KafkaInfo])

case class TopicAndConsumersDetailsWrapper(consumers: TopicAndConsumersDetails)

case class ConsumerDetail(name: String)

case class KafkaInfo(name: String, brokers: Seq[Node], offsets: Seq[OffsetInfo])
