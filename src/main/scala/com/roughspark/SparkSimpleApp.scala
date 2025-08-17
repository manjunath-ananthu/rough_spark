package com.roughspark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

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
          { "$limit": 100 }
        ]
        """
        )
        .load()
      val transformedDF = mongoDF.withColumn("created", current_timestamp())
      transformedDF.show()
      spark.sql("CREATE DATABASE IF NOT EXISTS rough_db")
      spark.catalog.setCurrentDatabase("rough_db")
      // Write to Hive as a managed table (overwrite for idempotency in dev)
      transformedDF
        .limit(200)
        .write
        .mode("append")
        .format("parquet")
        .saveAsTable("client_company_tbl")
      println("Written to Hive table rough_db.client_company_tbl in Parquet format")
      // Store billing_address as Parquet
      storeBillingAddressAsParquet(transformedDF, "/tmp/billing_address_parquet")
    } finally {
      spark.stop()
    }
  }

  def storeBillingAddressAsParquet(df: DataFrame, parquetPath: String): Unit = {
    df.select("billing_address")
      .write
      .mode("overwrite")
      .parquet(parquetPath)
    println(s"billing_address written to Parquet at $parquetPath")
  }
}
