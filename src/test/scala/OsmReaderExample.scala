import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object OsmReaderExample {
  private def getQty(df: DataFrame)(osmType: String): Long = df.filter(col("TYPE") === osmType).select("count").collect().head.getAs[Long](0)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[4]")
      .config("spark.executor.memory", "4gb")
      .getOrCreate()

    val sourceFile = new File(args(0))
    spark.sparkContext.addFile(sourceFile.getAbsolutePath)
    val osm = spark.read
      .option("threads", 6)
      .option("partitions", 8)
      .format("akashihi.osm.spark.OsmSource")
      .load(sourceFile.getName).drop("INFO")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val combined = osm.withColumn("TYPE", when(col("LAT").isNotNull, lit("NODE")).when(col("WAY").isNotNull, lit("WAY")).when(col("RELATION").isNotNull, lit("RELATION")))

    val counted = combined.groupBy("TYPE").count()

    val objectCountGet = getQty(counted)(_)
    val nodes = objectCountGet("NODE")
    val ways = objectCountGet("WAY")
    val relations = objectCountGet("RELATION")
    
    println(s"Nodes: $nodes, Ways: $ways, Relations: $relations, partitions: ${osm.rdd.partitions.length}")
  }
}
