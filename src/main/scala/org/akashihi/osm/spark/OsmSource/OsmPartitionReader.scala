package org.akashihi.osm.spark.OsmSource

import java.util.concurrent._
import java.util.function.Consumer

import org.akashihi.osm.parallelpbf.ParallelBinaryParser
import org.akashihi.osm.parallelpbf.entity._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._
import scala.collection.mutable

class OsmPartitionReader(input: String, schema: StructType, threads: Int, partitionsNo: Int, partition: Int) extends InputPartitionReader[InternalRow] {
  private val schemaColumnNames = schema.fields.map(_.name)

  private val parserTask = new FutureTask[Unit](new Callable[Unit]() {
    override def call: Unit = {
      val spark = SparkSession.getDefaultSession.get
      val source = new Path(input)
      val fs = source.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val inputStream = fs.open(source)
      val parser = new ParallelBinaryParser(inputStream, threads, partitionsNo, partition)

      if (schemaColumnNames.exists(field => field.equalsIgnoreCase("LAT") || field.equals("LON"))) {
        parser.onNode(onNode)
      }
      if (schemaColumnNames.exists(_.equalsIgnoreCase("WAY"))) {
        parser.onWay(onWay)
      }
      if (schemaColumnNames.exists(_.equalsIgnoreCase("RELATION"))) {
        parser.onRelation(onRelation)
      }

      parser.parse()
    }
  })
  private var parseThread: Thread = _
  private val queue = new SynchronousQueue[InternalRow]
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
    val stringifiedTags = tags.toMap.flatMap(kv => Map(UTF8String.fromString(kv._1.toLowerCase) -> UTF8String.fromString(kv._2)))
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

  def makeRowPreamble(entity: OsmEntity): mutable.MutableList[Any] = {
    val content = mutable.MutableList[Any]()
    if (schemaColumnNames.exists(_.equalsIgnoreCase("ID"))) {
      content += entity.getId
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("TAG"))) {
      content += makeTags(entity.getTags)
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("INFO"))) {
      content += makeInfo(entity)
    }
    content
  }

  def callback[T <: OsmEntity](handler: T => mutable.MutableList[Any]): Consumer[T] = {
    new Consumer[T] {
      override def accept(t: T): Unit = {
        val content = makeRowPreamble(t) ++= handler(t)
        val row = InternalRow.fromSeq(content)
        queue.offer(row, 1, TimeUnit.SECONDS)
      }
    }
  }

  private val onNode = callback[Node](t => {
    val content = mutable.MutableList[Any]()
    if (schemaColumnNames.exists(_.equalsIgnoreCase("TYPE"))) {
      content += RelationMember.Type.NODE.ordinal()
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("LAT"))) {
      content += t.getLat
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("LON"))) {
      content += t.getLon
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("WAY"))) {
      content += null
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("RELATION"))) {
      content += null
    }
    content
  })

  private val onWay = callback[Way](t => {
    val content = mutable.MutableList[Any]()
    if (schemaColumnNames.exists(_.equalsIgnoreCase("TYPE"))) {
      content += RelationMember.Type.WAY.ordinal()
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("LAT"))) {
      content += null
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("LON"))) {
      content += null
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("WAY"))) {
      content += ArrayData.toArrayData(t.getNodes.toArray)
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("RELATION"))) {
      content += null
    }
    content
  })

  private val onRelation = callback[Relation](t => {
    val content = mutable.MutableList[Any]()
    val members = t.getMembers.toSeq.map(member => {
      val role = if (member.getRole != null) {
        UTF8String.fromString(member.getRole)
      } else {
        null
      }
      InternalRow(member.getId, role, member.getType.ordinal())
    })

    if (schemaColumnNames.exists(_.equalsIgnoreCase("TYPE"))) {
      content += RelationMember.Type.RELATION.ordinal()
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("LAT"))) {
      content += null
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("LON"))) {
      content += null
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("WAY"))) {
      content += null
    }
    if (schemaColumnNames.exists(_.equalsIgnoreCase("RELATION"))) {
      content += ArrayData.toArrayData(members)
    }
    content
  })
}
