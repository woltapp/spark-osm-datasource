import com.wolt.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OsmReaderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[4]")
      .config("spark.executor.memory", "4gb")
      .getOrCreate()

    val osm = spark.read
      .option("threads", 6)
      .option("partitions", 32)
      .format(OsmSource.OSM_SOURCE_NAME)
      .load(args(0)).drop("INFO")


    val counted = osm.filter(col("TAG")("fixme").isNotNull).groupBy("TYPE").count().collect()

    val nodes = counted.filter(r => r.getAs[Int]("TYPE") == 0).head.getAs[Long]("count")
    val ways = counted.filter(r => r.getAs[Int]("TYPE") == 1).head.getAs[Long]("count")
    val relations = counted.filter(r => r.getAs[Int]("TYPE") == 2).head.getAs[Long]("count")

    println(s"Nodes: $nodes, Ways: $ways, Relations: $relations, partitions: ${osm.rdd.partitions.length}")
  }
}
