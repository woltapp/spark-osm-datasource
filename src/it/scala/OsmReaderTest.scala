import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.scalactic.Tolerance._
import org.scalatest._

import scala.collection.mutable

class OsmReaderTest extends FunSuite with BeforeAndAfter {
  def dataframe: DataFrame = {
    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[4]")
      .config("spark.executor.memory", "4gb")
      .getOrCreate()

    val path = getClass.getResource("sample.pbf").getPath
    spark.read
      .option("threads", 6)
      .option("partitions", 8)
      .format("akashihi.osm.spark.OsmSource")
      .load(path).persist(StorageLevel.MEMORY_AND_DISK)
  }

  test("Simple node should be read with correct coordinates") {
    val simpleNode = dataframe.filter(col("ID") === 653970877).collect().head

    assert(simpleNode.getAs[Int]("TYPE") == 0)
    assert(simpleNode.getAs[Double]("LAT") === 51.7636027 +- 0.0000001)
    assert(simpleNode.getAs[Double]("LON") === -0.228757 +- 0.0000001)
    assert(simpleNode.getAs[Map[String, String]]("TAG").isEmpty)

    val info = simpleNode.getAs[Row]("INFO")
    assert(info.getAs[Int]("UID") == 234999)
    assert(info.getAs[String]("USERNAME") === "Nicholas Shanks")
    assert(info.getAs[Int]("VERSION") == 1)
    assert(info.getAs[Long]("TIMESTAMP") == 1267144226000L)
    assert(info.getAs[Long]("CHANGESET") == 3977001)
    assert(info.getAs[Boolean]("VISIBLE"))
  }

  test("Tagged node should have correct coordinates and tags") {
    val taggedNode = dataframe.filter(col("ID") === 502550970).collect().head

    assert(taggedNode.getAs[Int]("TYPE") == 0)
    assert(taggedNode.getAs[Double]("LAT") === 51.7651177 +- 0.0000001)
    assert(taggedNode.getAs[Double]("LON") === -0.2336668 +- 0.0000001)
    assert(taggedNode.getAs[Map[String, String]]("TAG").nonEmpty)

    val tags = taggedNode.getAs[Map[String, String]]("TAG")
    assert(tags.contains("name"))
    assert(tags.getOrElse("name", "") === "Oaktree Close")
    assert(tags.contains("highway"))
    assert(tags.getOrElse("highway", "") === "bus_stop")

    val info = taggedNode.getAs[Row]("INFO")
    assert(info.getAs[Int]("UID") == 104459)
    assert(info.getAs[String]("USERNAME") === "NaPTAN")
    assert(info.getAs[Int]("VERSION") == 1)
    assert(info.getAs[Long]("TIMESTAMP") == 1253397762000L)
    assert(info.getAs[Long]("CHANGESET") == 2539009)
    assert(info.getAs[Boolean]("VISIBLE"))
  }

  test("Tagged way should have tags and consist of correct nodes") {
    val taggedWay = dataframe.filter(col("ID") === 158788812).collect().head

    assert(taggedWay.getAs[Int]("TYPE") == 1)
    assert(taggedWay.getAs[mutable.WrappedArray[Long]]("WAY") == Seq(1709246789L, 1709246746L, 1709246741L, 1709246791L))
    assert(taggedWay.getAs[Map[String, String]]("TAG").nonEmpty)

    val tags = taggedWay.getAs[Map[String, String]]("TAG")
    assert(tags.contains("highway"))
    assert(tags.getOrElse("highway", "") === "footway")

    val info = taggedWay.getAs[Row]("INFO")
    assert(info.getAs[Int]("UID") == 470302)
    assert(info.getAs[String]("USERNAME") === "Kjc")
    assert(info.getAs[Int]("VERSION") == 1)
    assert(info.getAs[Long]("TIMESTAMP") == 1334007464L)
    assert(info.getAs[Long]("CHANGESET") == 11245909)
    assert(!info.getAs[Boolean]("VISIBLE"))
  }

  test("Tagged relation should have tags and members") {
    val taggedRelation = dataframe.filter(col("ID") === 31640).collect().head

    assert(taggedRelation.getAs[Int]("TYPE") == 2)
    assert(taggedRelation.getAs[Map[String, String]]("TAG").nonEmpty)

    val members = taggedRelation.getAs[mutable.WrappedArray[Row]]("RELATION")
    val member = members.find(row => row.getAs[Long]("ID") == 25896432)
    assert(member.nonEmpty)
    assert(member.map(_.getAs[String]("ROLE")).getOrElse("") === "forward")
    assert(member.map(_.getAs[Int]("TYPE")).getOrElse(-1) === 1)

    val tags = taggedRelation.getAs[Map[String, String]]("TAG")
    assert(tags.contains("route"))
    assert(tags.getOrElse("route", "") === "bicycle")

    val info = taggedRelation.getAs[Row]("INFO")
    assert(info.getAs[Int]("UID") == 24119)
    assert(info.getAs[String]("USERNAME") === "Mauls")
    assert(info.getAs[Int]("VERSION") == 81)
    assert(info.getAs[Long]("TIMESTAMP") == 1337419064L)
    assert(info.getAs[Long]("CHANGESET") == 11640673)
    assert(!info.getAs[Boolean]("VISIBLE"))
  }
}
