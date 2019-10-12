import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object OsmReaderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[24]")
      .config("spark.testing.memory", "17179869184")
      .getOrCreate()

    val sourceFile = new File(args(0))
    spark.sparkContext.addFile(sourceFile.getAbsolutePath)
    val osm = spark.read.option("threads", 1).option("partitions", 256).format("akashihi.osm.spark.OsmSource").load(sourceFile.getName).drop("INFO").persist(StorageLevel.MEMORY_AND_DISK)
    val nodes = osm.filter(col("LAT").isNotNull).count()
    val ways = osm.filter(col("WAY").isNotNull).count()
    val relations = osm.filter(col("RELATION").isNotNull).count()

    println(s"Nodes: $nodes, Ways: $ways, Relations: $relations, partitions: ${osm.rdd.partitions.length}")
  }
}
