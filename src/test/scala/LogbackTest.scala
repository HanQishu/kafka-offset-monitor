import org.scalatest.FunSuite

/**
  * Created by huangxingbiao@github.com on 24/07/2019.
  */
class LogbackTest extends FunSuite {

  import org.slf4j.{Logger, LoggerFactory}

  // val LOG: Logger = LoggerFactory.getLogger(classOf[LogbackTest])
  val LOGONE: Logger = LoggerFactory.getLogger(classOf[LogbackTest])

  test("logback test") {

    LOGONE.trace("Hello World!")
    LOGONE.debug("How are you today?")
    LOGONE.info("I am fine.")
    LOGONE.warn("I love programming.")
    LOGONE.error("I am programming.")

  }

}
