ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

ThisBuild / organization := "com.roughspark"

lazy val root = (project in file("."))
  .settings(
    name := "rough_spark",
    scalaVersion := "2.12.19",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.scala-lang" %% "toolkit-test" % "0.1.7" % Test,
      "org.apache.spark" %% "spark-core" % "3.5.1",
      // Enable Hive support in Spark (matches Spark version)
      "org.apache.spark" %% "spark-hive" % "3.5.1",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.5.0"
    ),
    // Add JVM options for Java 11+ compatibility
  )

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

ThisBuild / assemblyJarName := "rough-spark-assembly-1.0.jar"
