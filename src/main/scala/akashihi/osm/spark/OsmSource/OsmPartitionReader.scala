package akashihi.osm.spark.OsmSource

import java.io.FileInputStream
import java.util.concurrent._
import java.util.function.Consumer

import akashihi.osm.parallelpbf.ParallelBinaryParser
import akashihi.osm.parallelpbf.entity.{Node, OsmEntity, Relation, RelationMember, Way}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._

class OsmPartitionReader(input: String, partitionsNo: Int, partition: Int) extends InputPartitionReader[InternalRow] {
  private val parserTask = new FutureTask[Unit](new Callable[Unit]() {
    override def call: Unit = {
      val inputStream = new FileInputStream(input)
      val parser = new ParallelBinaryParser(inputStream, 1, partitionsNo, partition)
        .onNode(onNode)
        .onWay(onWay)
        .onRelation(onRelation)
      parser.parse()
    }
  })
  private var parseThread: Thread = _
  private val queue = new SynchronousQueue[InternalRow] //Nine is picked absolutely randomly
  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (parseThread == null) {
      parseThread = new Thread(parserTask)
      parseThread.start()
    }
    while (!parserTask.isDone) {
      currentRow = queue.poll(1, TimeUnit.SECONDS)
      if (currentRow != null) {
        return true
      }
    }
    false
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    parserTask.cancel(true)
  }

  def makeTags(tags: java.util.Map[String, String]): MapData = {
    val stringifiedTags = tags.toMap.flatMap(kv => Map(UTF8String.fromString(kv._1) -> UTF8String.fromString(kv._2)))
    ArrayBasedMapData(stringifiedTags)
  }

  def makeInfo(entity: OsmEntity): InternalRow = {
    if (entity.getInfo != null) {
      val info = entity.getInfo
      val username = if (info.getUsername != null) {
        UTF8String.fromString(info.getUsername)
      } else {
        null
      }
      InternalRow(info.getUid, username, info.getVersion, info.getTimestamp, info.getChangeset, info.isVisible)
    } else {
      null
    }
  }

  private val onNode = new Consumer[Node] {
    override def accept(t: Node): Unit = {
      val row = InternalRow(t.getId, makeTags(t.getTags), makeInfo(t), t.getLat, t.getLon, null, null)
      queue.offer(row, 1, TimeUnit.SECONDS)
    }
  }

  private val onWay = new Consumer[Way] {
    override def accept(t: Way): Unit = {
      val row = InternalRow(t.getId, makeTags(t.getTags), makeInfo(t), null, null, ArrayData.toArrayData(t.getNodes.toArray), null)
      queue.offer(row, 1, TimeUnit.SECONDS)
    }
  }

  private val onRelation = new Consumer[Relation] {
    override def accept(t: Relation): Unit = {
      val members = t.getMembers.toSeq.map(member => {
        val role = if (member.getRole != null) {
          UTF8String.fromString(member.getRole)
        } else {
          null
        }
        InternalRow(member.getId, role, member.getType.ordinal())
      })
      val row = InternalRow(t.getId, makeTags(t.getTags), makeInfo(t), null, null, null, ArrayData.toArrayData(members))
      queue.offer(row, 1, TimeUnit.SECONDS)
    }
  }
}
