ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Structured_Api_referring_columns"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-avro" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-hive" % "3.0.0-preview2")