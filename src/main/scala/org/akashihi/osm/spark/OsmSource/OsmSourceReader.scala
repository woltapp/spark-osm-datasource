package org.akashihi.osm.spark.OsmSource

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._
import scala.util.Try

class OsmSourceReader(input: String, partitions: String, threads: String) extends DataSourceReader with SupportsPushDownRequiredColumns {
  private var requiredSchema = OsmSource.schema

  override def readSchema(): StructType = requiredSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitionsNo = Try(partitions.toInt).getOrElse(1)
    val threadsNo = Try(threads.toInt).getOrElse(1)
    val shiftedPartitions = partitionsNo -1
    (0 to shiftedPartitions).map(p => new OsmPartition(input, this.requiredSchema, threadsNo, partitionsNo,  p)).toList
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
