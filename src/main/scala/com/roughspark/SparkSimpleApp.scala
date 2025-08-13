package com.roughspark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import com.mongodb.spark.connector._
import com.mongodb.spark.sql._

object SparkSimpleApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark MongoDB to Hive")
      .setMaster("spark://spark-master:7077")
      .set("spark.mongodb.read.connection.uri", "mongodb://mongo_service:27017")
      .set("spark.mongodb.read.database", "cf_stage_v2")
      .set("spark.mongodb.read.collection", "client_company")

    val spark = SparkSession.builder()
      .config(conf)
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    try {
      val mongoDF: DataFrame = spark.read
        .format("mongodb")
        .option("batchSize", "10")
        .option(
          "aggregation.pipeline",
          """
        [
          { "$match": { "company_id": "542ab2e523df4255a24e71a719f3da85" } },
          { "$limit": 10 }
        ]
        """
        )
        .load()
      val transformedDF = mongoDF.withColumn("created", current_timestamp())
      transformedDF.show()
      spark.sql("CREATE DATABASE IF NOT EXISTS rough_db")
      spark.catalog.setCurrentDatabase("rough_db")

      // Show some data
      println()
      println()
      println()
      println()
      println(s"Total records: ${mongoDF.count()}")
      println()
      println()
      println()
      println()

      // Write to Hive as a managed table (overwrite for idempotency in dev)
      transformedDF
        .limit(200)
        .write
        .mode("overwrite")
        .format("hive")
        .saveAsTable("client_company_tbl")
      println("Written to Hive table rough_db.client_company_tbl")

    } finally {
      spark.stop()
    }
  }
}
