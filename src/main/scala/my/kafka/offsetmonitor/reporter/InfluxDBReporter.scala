package my.kafka.offsetmonitor.reporter

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import kafka.utils.Logging
import my.kafka.offsetmonitor.Reporter
import my.kafka.offsetmonitor.bean.OffsetInfo
import my.kafka.offsetmonitor.common.Common
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.{BatchOptions, InfluxDB, InfluxDBFactory}

import scala.concurrent.duration.Duration

/**
  * Created by huangxingbiao on 2019/1/11.
  */
class InfluxDBReporter extends Reporter with Logging {

  var influxdb: InfluxDB = _

  var database: String = _
  var offset_msm: String = _
  var logsize_msm: String = _
  var refresh: Duration = _

  override def initialize(configs: Map[String, _]): Unit = {
    info("initialize influxdb reporter....")

    database = configs("database").toString
    val measurement = configs("measurement").asInstanceOf[java.util.Map[String, String]]
    logsize_msm = measurement.get("logsize")
    offset_msm = measurement.get("offset")

    refresh = Duration(configs("refresh").toString)

    val url = configs("url").toString
    val username = configs.getOrElse("username", "").toString
    val password = configs.getOrElse("password", "").toString


    influxdb = if (username.isEmpty) InfluxDBFactory.connect(url) else InfluxDBFactory.connect(url, username, password)

    influxdb.createDatabase(database)
    influxdb.setDatabase(database)
    influxdb.enableBatch(BatchOptions.DEFAULTS)


    val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

    implicit def funToRunnable(fun: () => Unit): Runnable = new Runnable() {
      def run(): Unit = fun()
    }

    scheduler.scheduleAtFixedRate(
      () => {
        val offsetInfoes = offset().flatMap(_._2.values)
        info(s"reporting ${offsetInfoes.size}")
        report(offsetInfoes.toIndexedSeq)
      },
      10000, refresh.toMillis, TimeUnit.MILLISECONDS)
  }

  def report(infoes: IndexedSeq[OffsetInfo]): Unit = {
    Common.retryTask {
      reportOffset(infoes)
    }
    Common.retryTask {
      reportLogSize(infoes)
    }
  }

  def reportOffset(infoes: IndexedSeq[OffsetInfo]): Unit = {
    val batch: BatchPoints = BatchPoints.database(database).build()

    infoes.foreach(offset => {
      val point = Point.measurement(offset_msm)
        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .tag("group", offset.group)
        .tag("topic", offset.topic)
        .tag("partition", offset.partition.toString)
        .addField("offset", offset.offset)
        .addField("logsize", offset.logSize)
        .addField("lag", offset.lag)
        .build()
      batch.point(point)
    })

    influxdb.write(batch)
  }

  def reportLogSize(infoes: IndexedSeq[OffsetInfo]): Unit = {
    val batch: BatchPoints = BatchPoints.database(database).build()

    infoes.map(i => (i.topic, i.partition, i.logSize)).toSet[(String, Int, Long)]
      .foreach(offset => {
        val point = Point.measurement(logsize_msm)
          .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
          .tag("topic", offset._1)
          .tag("partition", offset._2.toString)
          .addField("logsize", offset._3)
          .build()
        batch.point(point)
      })

    influxdb.write(batch)
  }

}
