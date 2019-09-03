package my.kafka.offsetmonitor

import my.kafka.offsetmonitor.bean.OffsetInfo
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by huangxingbiao on 2019/1/9.
  */
trait MonitorCenter extends Initializable {

  val reporters: mutable.Buffer[Reporter] = new ListBuffer[Reporter]()

  def register(reporter: Reporter): Reporter = {
    if (!reporters.contains(reporter)) {
      reporters.append(reporter)
      reporter.monitor = this
    }
    reporter
  }

  val offsetInfoMap: mutable.Map[TopicPartition, mutable.Map[String, OffsetInfo]] = mutable.HashMap()

  def monitoring()

}
