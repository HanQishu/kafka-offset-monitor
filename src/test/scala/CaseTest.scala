import java.lang.reflect.Method
import java.time.Duration

import com.sw.kafka.offsetmonitor.bean.Monitors
import kafka.coordinator.group.GroupTopicPartition
import my.kafka.offsetmonitor.bean.Monitors
import my.kafka.offsetmonitor.center.KafkaMonitorCenter
import my.kafka.offsetmonitor.common.KafkaCreator
import org.apache.kafka.common.TopicPartition
import org.scalatest.FunSuite
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.collection.JavaConverters._

/**
  * Created by huangxingbiao@github.com on 20/06/2019.
  */
class CaseTest extends FunSuite {

  val broker = "node9:9092"
  val admin = KafkaCreator.admin(broker)
  val topic = "csv_test001"

  def reflectMethod(methodName: String, clazz: Class[_], argsClassType: Class[_]*): Method = {
    clazz.getDeclaredMethod(methodName, argsClassType: _*)
  }

  def initMonitorCenter(): KafkaMonitorCenter = {
    val inputstream = this.getClass.getResourceAsStream("/monitors.yaml")
    val yaml = new Yaml(new Constructor(classOf[Monitors]))
    val config: Monitors = yaml.load(inputstream).asInstanceOf[Monitors]

    val monitorCenter = new KafkaMonitorCenter()
    monitorCenter.initialize(config.monitors.get(0).center.asScala.toMap)
    monitorCenter
  }

  test("reflect method") {
    val name = "com$sw$kafka$offsetmonitor$KafkaMonitorCenter$$topicExists"
    val method = reflectMethod(name, classOf[KafkaMonitorCenter], classOf[String])
    assert(method.getName == name)
  }

  test("check KafkaMonitorCenter topicExists method") {
    val name = "com$sw$kafka$offsetmonitor$KafkaMonitorCenter$$topicExists"
    val method = reflectMethod(name, classOf[KafkaMonitorCenter], classOf[String])

    val monitorCenter = initMonitorCenter()

    var result = method.invoke(monitorCenter, topic).asInstanceOf[Boolean]
    assert(result)

    result = method.invoke(monitorCenter, topic).asInstanceOf[Boolean]
    assert(!result)
  }

  test("check KafkaMonitorCenter isExclude method") {
    val name = "com$sw$kafka$offsetmonitor$center$KafkaMonitorCenter$$isExclude"
    val method = reflectMethod(name, classOf[KafkaMonitorCenter], classOf[GroupTopicPartition])

    val monitorCenter = initMonitorCenter()

    def gtp(group: String, topic: String, partition: Int): GroupTopicPartition =
      GroupTopicPartition(group, new TopicPartition(topic, partition))

    var test = gtp("console-consumer-57453", "test", 1)
    var result = method.invoke(monitorCenter, test).asInstanceOf[Boolean]
    assert(result)

    test = gtp("abc", "__consumer_offsets", 1)
    result = method.invoke(monitorCenter, test).asInstanceOf[Boolean]
    assert(!result)

    test = gtp("eis_hadoop_invoke_logger_app_20190722", "test", 1)
    result = method.invoke(monitorCenter, test).asInstanceOf[Boolean]
    assert(result)
  }

  test("check KafkaMonitorCenter fetchLogSize") {
    val name = "com$sw$kafka$offsetmonitor$KafkaMonitorCenter$$fetchLogSize"
    val method = reflectMethod(name, classOf[KafkaMonitorCenter], classOf[TopicPartition])

    val monitorCenter = initMonitorCenter()
    val topicPartition = new TopicPartition(topic, 0)
    val logsize = method.invoke(monitorCenter, topicPartition).asInstanceOf[Long]
    assert(logsize > 0, s"topic $topic maybe not exists")
  }

  test("list topic") {
    println(admin.listTopics().names().get())
  }

  test("consume ls_test") {
    val consumer = KafkaCreator.consumer(broker, "ls_test")
    import scala.collection.JavaConversions._
    while (true) {
      for (record <- consumer.poll(Duration.ofMillis(5000L))) {
        println(record)
      }
      println("fetch ....")
    }

  }

}
