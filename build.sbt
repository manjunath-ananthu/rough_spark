ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

ThisBuild / organization := "com.manjunath"

lazy val root = (project in file("."))
  .settings(
    name := "rought_spark",
    scalaVersion := "2.12.19",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.scala-lang" %% "toolkit-test" % "0.1.7" % Test
    )
  )
