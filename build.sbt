organization := "com.wolt.osm"
homepage := Some(url("https://github.com/woltapp/spark-osm-datasource"))
scmInfo := Some(ScmInfo(url("https://github.com/woltapp/spark-osm-datasource"), "git@github.com:woltapp/spark-osm-datasource.git"))
developers := List(Developer("akashihi",
  "Denis Chaplygin",
  "denis.chaplygin@wolt.com",
  url("https://github.com/akashihi")))
licenses += ("GPLv3", url("https://www.gnu.org/licenses/gpl-3.0.txt"))
publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

name := "spark-osm-datasource"
version := "0.3.0"

scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.12.10")

val mavenLocal = "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
resolvers += mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "com.wolt.osm" % "parallelpbf" % "0.2.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "it,test",
  "org.scalactic" %% "scalactic" % "3.1.0" % "it,test"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)