package akashihi.osm.spark.OsmSource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, StringKeyHashMap}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

class OsmPartitionReader(input: String, partition: Int) extends InputPartitionReader[InternalRow] {
  var firstTime = true
  override def next(): Boolean = {
    if (firstTime) {
      firstTime = false
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    val tags = Map(UTF8String.fromString("tag")->UTF8String.fromString("value"))
    val mapData = ArrayBasedMapData(tags)
    val info = InternalRow(100500, UTF8String.fromString("test"), 1, 9000L, 362L, false)
    val ways = ArrayData.toArrayData(Array(1L, 2L, 3L))
    val relation_first = InternalRow(5L, null, 0)
    val relation_second = InternalRow(6L, UTF8String.fromString("test"), 1)
    val relations = ArrayData.toArrayData(Array(relation_first, relation_second))
    InternalRow(1L, mapData, info, 100d, 50d, ways, relations)
  }


  override def close(): Unit = {}
}
