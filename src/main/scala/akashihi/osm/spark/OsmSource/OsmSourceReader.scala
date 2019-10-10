package akashihi.osm.spark.OsmSource

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Try

class OsmSourceReader(input: String, partitions: String) extends DataSourceReader with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  var requiredSchema = OsmSource.schema
  var filters = Array.empty[Filter]
  var tagFilters = Array.empty[(String, String)]

  override def readSchema(): StructType = requiredSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitionsNo = Try(partitions.toInt).getOrElse(1)
    (1 to partitionsNo).map(p => new OsmPartition(input, p)).toList
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val supported = ListBuffer.empty[Filter]
    val unsupported = ListBuffer.empty[Filter]
    val tagFilters = ListBuffer.empty[(String, String)]

    filters.foreach {
      case filter: EqualTo => {
        supported += filter
        val tagFilter = (filter.attribute, filter.value.toString)
        tagFilters += tagFilter
      }
      case filter => unsupported += filter
    }

    this.filters = supported.toArray
    this.tagFilters = tagFilters.toArray
    unsupported.toArray
  }


  override def pushedFilters(): Array[Filter] = filters
}
