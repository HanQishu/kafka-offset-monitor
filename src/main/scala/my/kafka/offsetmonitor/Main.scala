package my.kafka.offsetmonitor

import kafka.utils.Logging
import my.kafka.offsetmonitor.bean.{Monitor, Monitors}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by huangxingbiao on 2019/1/9.
  */
object Main extends Logging {

  def createInstance[T](clazz: Class[T], configs: Map[String, _]): T = {
    try {
      clazz.getConstructor().newInstance()
    } catch {
      case e: NoSuchMethodException =>
        clazz.getConstructor(classOf[java.util.HashMap[String, _]]).newInstance(configs)
      case e: Exception => throw e
    }
  }

  def loadReporter(configs: Map[String, _]): Reporter = {
    val reportername: String = configs("type").toString
    assert(reportername != null && reportername.nonEmpty, "reporter config is error")

    val reporter = Class.forName(reportername)
    info(s"load reporter: $reportername success")
    val instance = createInstance(reporter.asSubclass(classOf[Reporter]), configs)
    instance.initialize(configs)
    instance
  }

  def loadMonitorCenter(configs: Map[String, _]): MonitorCenter = {
    val monitorname = configs("type").toString
    assert(monitorname != null && monitorname.nonEmpty, "center config is error")

    val monitor = Class.forName(monitorname)
    info(s"load monitor: $monitorname success")
    val instance = createInstance(monitor.asSubclass(classOf[MonitorCenter]), configs)
    instance.initialize(configs)
    instance
  }

  def initializeYaml(yamlPath: String): mutable.Buffer[Monitor] = {
    val inputstream = this.getClass.getResourceAsStream(s"/$yamlPath")
    val yaml = new Yaml(new Constructor(classOf[Monitors]))
    val config: Monitors = yaml.load(inputstream).asInstanceOf[Monitors]

    inputstream.close()
    info(s"load properties: $yamlPath success")

    config.monitors.asScala
  }

  def main(args: Array[String]): Unit = {
    // assert(args.length == 1, "please input monitor.properties")
    val monitors = initializeYaml(if (args.length > 0) args(0) else "monitors.yaml")

    monitors.foreach { monitor => {
      val monitorCenter = loadMonitorCenter(monitor.center.asScala.toMap)

      monitor.reporters.asScala.foreach {
        config: java.util.Map[String, _] =>
          monitorCenter.register(loadReporter(config.asScala.toMap))
      }

      monitorCenter.monitoring()
    }
    }
  }

}
