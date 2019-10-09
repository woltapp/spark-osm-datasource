name := "spark-osm-reader"

version := "0.1"

scalaVersion := "2.11.12"

val mavenLocal = "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
resolvers += mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "akashihi.osm" % "parallelpbf" % "0.1.0"
)

