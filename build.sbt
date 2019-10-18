organization := "akashihi.osm"
homepage := Some(url("https://github.com/akashihi/spark-osm-reader"))
scmInfo := Some(ScmInfo(url("https://github.com/akashihi/spark-osm-reader"), "git@github.com:akashihi/spark-osm-reader.git"))
developers := List(Developer("akashihi",
  "Denis Chaplygin",
  "akashihi@gmail.com",
  url("https://github.com/akashihi")))
licenses += ("GPLv3", url("https://www.gnu.org/licenses/gpl-3.0.txt"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

name := "spark-osm-reader"
version := "0.1.0"

scalaVersion := "2.11.12"

val mavenLocal = "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
resolvers += mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "akashihi.osm" % "parallelpbf" % "0.1.0"
)

