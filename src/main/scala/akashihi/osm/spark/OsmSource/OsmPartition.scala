package akashihi.osm.spark.OsmSource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class OsmPartition(input: String, schema: StructType, threads: Int, partitionsNo: Int, partition: Int) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new OsmPartitionReader(input, schema, threads, partitionsNo, partition)
}
