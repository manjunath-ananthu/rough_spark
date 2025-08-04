package com.roughspark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import com.mongodb.spark.connector._
import com.mongodb.spark.sql._

object SparkSimpleApp {
  def main(args: Array[String]): Unit = {
    // Create SparkConf with MongoDB configuration
    val conf = new SparkConf()
      .setAppName("Spark MongoDB to Hive")
      .setMaster("spark://spark-master:7077")
      .set("spark.mongodb.read.connection.uri", "mongodb://mongo_service:27017")
      .set("spark.mongodb.read.database", "cf_stage_v2")
      .set("spark.mongodb.read.collection", "client_company")

    // Create SparkSession with the configuration
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    try {
      // Read from MongoDB
      val mongoDF: DataFrame = spark.read
        .format("mongodb")
        .load()

      // Perform transformations
      val transformedDF = mongoDF.withColumn("created", current_timestamp())

      // Show some data
      transformedDF.show(5)
      println(s"Total records: ${mongoDF.count()}")

    } finally {
      spark.stop()
    }
  }
}
