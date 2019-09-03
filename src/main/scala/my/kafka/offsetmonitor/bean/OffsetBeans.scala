package my.kafka.offsetmonitor.bean

import scala.collection.Seq

case class OffsetInfo(group: String,
                      topic: String,
                      partition: Int,
                      offset: Long,
                      var logSize: Long,
                      modified: Long) {
  def lag: Long = logSize - offset
}

object OffsetDB {

  case class OffsetPoints(timestamp: Long, partition: Int, offset: Long, logSize: Long)

  case class OffsetHistory(group: String, topic: String, offsets: Seq[OffsetPoints])

  case class DbOffsetInfo(id: Option[Int] = None, timestamp: Long, offset: OffsetInfo)

  object DbOffsetInfo {
    def parse(in: (Option[Int], String, String, Int, Long, Long, Long, Long, Long)): DbOffsetInfo = {
      val (id, group, topic, partition, offset, logSize, timestamp, creation, modified) = in
      DbOffsetInfo(id, timestamp, OffsetInfo(group, topic, partition, offset, logSize, modified))
    }

    def unparse(in: DbOffsetInfo): Option[(Option[Int], String, String, Int, Long, Long, Long, Long)] = Some(
      in.id,
      in.offset.group,
      in.offset.topic,
      in.offset.partition,
      in.offset.offset,
      in.offset.logSize,
      in.timestamp,
      in.offset.modified
    )
  }

}
