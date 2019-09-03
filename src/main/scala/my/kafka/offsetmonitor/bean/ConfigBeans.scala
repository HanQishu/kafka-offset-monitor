package my.kafka.offsetmonitor.bean

import scala.beans.BeanProperty

class Monitor extends Serializable {
  @BeanProperty var center: java.util.Map[String, _] = null
  @BeanProperty var reporters: java.util.List[java.util.Map[String, _]] = null
}

class Monitors extends Serializable {
  @BeanProperty var monitors: java.util.List[Monitor] = null
}