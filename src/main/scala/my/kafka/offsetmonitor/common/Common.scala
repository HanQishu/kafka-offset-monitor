package my.kafka.offsetmonitor.common

import java.util.{Collections, Properties}

import kafka.utils.Logging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by huangxingbiao on 2019/1/9.
  */
object Common extends Logging {

  var groupId: String = "kafka-offset-monitor1"

  def retryTask[T](fn: => T) {
    try {
      retry(3) {
        fn
      }
    } catch {
      case NonFatal(e) =>
        error("Failed to run scheduled task", e)
    }
  }

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }

  def subProps(props: Properties, prefix: String): Properties = {
    val subProperties = new Properties()
    props.stringPropertyNames().asScala.filter(_.startsWith(s"$prefix.")).foreach {
      name => subProperties.setProperty(name.substring(prefix.length + 1), props.getProperty(name))
    }
    subProperties
  }

}

object KafkaCreator {

  def consumer(bootstrap: String, topic: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props: Properties = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG, Common.groupId)
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    consumer.subscribe(Collections.singleton(topic))
    consumer
  }

  def admin(bootstrap: String): AdminClient = {
    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    AdminClient.create(props)
  }

}