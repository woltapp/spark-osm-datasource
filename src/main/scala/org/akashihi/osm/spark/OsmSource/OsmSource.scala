package org.akashihi.osm.spark.OsmSource

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._

object OsmSource {
  val OSM_SOURCE_NAME = "org.akashihi.osm.spark.OsmSource"

  private val info = Seq(
    StructField("UID", IntegerType, nullable = true),
    StructField("USERNAME", StringType, nullable = true),
    StructField("VERSION", IntegerType, nullable = true),
    StructField("TIMESTAMP", LongType, nullable = true),
    StructField("CHANGESET", LongType, nullable = true),
    StructField("VISIBLE", BooleanType, nullable = false)
  )

  private val member = Seq(
    StructField("ID", LongType, nullable = false),
    StructField("ROLE", StringType, nullable = true),
    StructField("TYPE", IntegerType, nullable = false)
  )

  private val fields = Seq(
    StructField("ID", LongType, nullable = false),
    StructField("TAG", MapType(StringType, StringType, valueContainsNull = false), nullable = false),
    StructField("INFO", StructType(info), nullable = true),
    StructField("TYPE", IntegerType, nullable = false),
    StructField("LAT", DoubleType, nullable = true),
    StructField("LON", DoubleType, nullable = true),
    StructField("WAY", ArrayType(LongType, containsNull = false), nullable = true),
    StructField("RELATION", ArrayType(StructType(member), containsNull = false), nullable = true)
  )

  val schema = StructType(fields)
}

class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = new OsmSourceReader(options.get("path").get, options.get("partitions").orElse("1"), options.get("threads").orElse("1"))
}