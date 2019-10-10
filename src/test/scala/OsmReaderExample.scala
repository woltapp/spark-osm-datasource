import org.apache.spark.sql.SparkSession

object OsmReaderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[24]")
      .config("spark.testing.memory", "17179869184")
      .getOrCreate()

    val osm = spark.read.option("input", "/home/cholya/Dropbox/Work/parallelpbf/src/test/resources/sample.pbf").option("partitions", 1).format("akashihi.osm.spark.OsmSource").load()
    osm.show(truncate = false)
  }
}
