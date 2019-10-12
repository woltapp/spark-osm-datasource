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

    spark.sparkContext.addFile("/home/chollya/maps/cities/extract-cesko-brno.osm.pbf")
    val osm = spark.read.option("threads", 1).option("partitions", 256).format("akashihi.osm.spark.OsmSource").load("extract-cesko-brno.osm.pbf").drop("INFO").persist(StorageLevel.MEMORY_AND_DISK)
    val nodes = osm.filter(col("LAT").isNotNull).count()
    val ways = osm.filter(col("WAY").isNotNull).count()
    val relations = osm.filter(col("RELATION").isNotNull).count()

    println(s"Nodes: $nodes, Ways: $ways, Relations: $relations, partitions: ${osm.rdd.partitions.length}")
  }
}
