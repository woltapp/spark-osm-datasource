package akashihi.osm.spark.OsmSource

import java.io.InputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

class OsmPartition(input: String, partition: Int) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new OsmPartitionReader(input, partition)
}
