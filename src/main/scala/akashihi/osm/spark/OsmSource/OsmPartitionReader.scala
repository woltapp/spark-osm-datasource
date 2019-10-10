package akashihi.osm.spark.OsmSource

import java.io.FileInputStream
import java.util.concurrent.{Callable, FutureTask, LinkedBlockingQueue, TimeUnit}
import java.util.function.Consumer

import akashihi.osm.parallelpbf.ParallelBinaryParser
import akashihi.osm.parallelpbf.entity.{Info, Node}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._

class OsmPartitionReader(input: String, partitionsNo: Int, partition: Int) extends InputPartitionReader[InternalRow] {
  private var parserTask = new FutureTask[Unit](new Callable[Unit]() {
    override def call: Unit = {
      val inputStream = new FileInputStream(input)
      val parser = new ParallelBinaryParser(inputStream, 1, partitionsNo, partition).onNode(onNode)
      parser.parse()
    }
  })
  private var parseThread: Thread = _
  private val queue = new LinkedBlockingQueue[InternalRow](9) //Nine is picked absolutely randomly
  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (parseThread == null) {
      parseThread = new Thread(parserTask)
      parseThread.start()
    }
    if (!parserTask.isDone || !queue.isEmpty) {
      currentRow = queue.poll(1, TimeUnit.SECONDS)
      if (currentRow != null) {
        return true
      }
    }
    false
  }

  override def get(): InternalRow = {
    currentRow
    /*queue.take()
    val tags = Map(UTF8String.fromString("tag") -> UTF8String.fromString("value"))
    val mapData = ArrayBasedMapData(tags)
    val info = InternalRow(100500, UTF8String.fromString("test"), 1, 9000L, 362L, false)
    val ways = ArrayData.toArrayData(Array(1L, 2L, 3L))
    val relation_first = InternalRow(5L, null, 0)
    val relation_second = InternalRow(6L, UTF8String.fromString("test"), 1)
    val relations = ArrayData.toArrayData(Array(relation_first, relation_second))
    InternalRow(1L, mapData, info, 100d, 50d, ways, relations)*/
  }


  override def close(): Unit = {}

  def makeTags(tags: java.util.Map[String, String]): MapData = {
    val stringifiedTags = tags.toMap.flatMap(kv => Map(UTF8String.fromString(kv._1) -> UTF8String.fromString(kv._2)))
    ArrayBasedMapData(stringifiedTags)
  }

  def makeInfo(info: Info): InternalRow = {
    val username = if (info.getUsername != null) {
      UTF8String.fromString(info.getUsername)
    } else {
      null
    }
    InternalRow(info.getUid, username, info.getVersion, info.getTimestamp, info.getChangeset, info.isVisible)
  }

  private val onNode = new Consumer[Node] {
    override def accept(t: Node): Unit = {
      val info = if (t.getInfo != null) {
        makeInfo(t.getInfo)
      } else {
        null
      }
      val row = InternalRow(t.getId, makeTags(t.getTags), info, t.getLat, t.getLon, null, null)
      queue.put(row)
    }
  }
}
