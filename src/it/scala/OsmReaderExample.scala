import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object OsmReaderExample {
  private def getQty(df: DataFrame)(osmType: Int): Long = df.filter(col("TYPE") === osmType).select("count").collect().head.getAs[Long](0)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[4]")
      .config("spark.executor.memory", "4gb")
      .getOrCreate()

    val osm = spark.read
      .option("threads", 6)
      .option("partitions", 8)
      .format("akashihi.osm.spark.OsmSource")
      .load(args(0)).drop("INFO")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val counted = osm.groupBy("TYPE").count()

    val objectCountGet = getQty(counted)(_)
    val nodes = objectCountGet(0)
    val ways = objectCountGet(1)
    val relations = objectCountGet(2)

    println(s"Nodes: $nodes, Ways: $ways, Relations: $relations, partitions: ${osm.rdd.partitions.length}")
  }
}
