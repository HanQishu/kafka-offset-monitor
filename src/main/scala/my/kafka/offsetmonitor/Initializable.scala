package my.kafka.offsetmonitor

/**
  * Created by huangxingbiao on 2019/1/15.
  */
trait Initializable {

  def initialize(configs: Map[String, _])

}
