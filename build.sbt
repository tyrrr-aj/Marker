name := "Marker"

version := "1.0"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" % "spark-streaming_2.12" % "2.4.3" % "provided",
  "org.apache.bahir" %% "spark-streaming-akka" % "2.4.0-SNAPSHOT"
)