name := "projectgtfs"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",

).map(_ % hadoopVersion)

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"
libraryDependencies += "com.opencsv" % "opencsv" % "5.1"