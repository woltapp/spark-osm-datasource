import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OsmReaderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[24]")
      .config("spark.testing.memory", "17179869184")
      .getOrCreate()

    val osm = spark.read.option("partitions", 10).format("akashihi.osm.spark.OsmSource").load("/home/chollya/Dropbox/Work/parallelpbf/src/test/resources/sample.pbf")
    osm.drop("RELATION", "INFO", "LAT", "LON").show(false)
  }
}
