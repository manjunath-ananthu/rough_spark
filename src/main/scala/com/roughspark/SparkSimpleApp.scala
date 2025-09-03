package com.roughspark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
        .option("batchSize", "200")
        .option(
          "aggregation.pipeline",
          """
        [
          { "$match": { "company_id": { "$ne": null }, "deleted": false } },
          { "$sort": { "created": -1 } },
          { "$skip": 20 },
          { "$limit": 1000 },
          { "$project": {
              "_id": 1,
              "created": 1,
              "updated": 1,
              "deleted": 1,
              "name": 1,
              "billing_address": 1,
              "company_id": 1,
              "owner_user_id": 1,
              "logo": 1,
              "sync_data": 1,
              "client_company_number": 1,
              "company_types": 1,
              "market": 1,
              "campaign": 1,
              "client_company_identifier": 1,
              "previously_updated_at": 1,
              "fax": 1,
              "website": 1,
              "linked_in_url": 1,
              "facebook_url": 1,
              "twitter_url": 1,
              "source": 1,
              "date_acquired": 1,
              "shipping_addresses": 1,
              "custom_fields": 1,
              "tax_region_id": 1,
              "read_only": 1,
              "archived": 1,
              "blocked_for_import_at": 1,
              "created_by_user_id": 1,
              "ms365_integration": 1,
              "cloud_assessment_counters": 1,
              "is_dummy_data": 1,
            }
          }
        ]
        """
        )
        .load()

        // Replace VOID fields with StringType (or drop them if not needed)
        val billingAddressCleaned = struct(
          col("billing_address._id").cast("string").alias("_id"),
          col("billing_address.address_line_1").cast("string").alias("address_line_1"),
          col("billing_address.address_line_2").cast("string").alias("address_line_2"),
          col("billing_address.pincode").cast("string").alias("pincode"),
          col("billing_address.city").cast("string").alias("city"),
          col("billing_address.country").cast("string").alias("country"),
          col("billing_address.state").cast("string").alias("state"),
          col("billing_address.phone").cast("string").alias("phone")
          // Exclude sync_data, is_default, email_addresses (VOID fields)
        )
      // Clean up sync_data array of structs using Scala API
      val syncDataCleaned = transform(
        col("sync_data"),
        element => struct(
          element.getField("synced_from").cast("string").alias("synced_from"),
          element.getField("sync_source_id").cast("string").alias("sync_source_id"),
          element.getField("synced_source_url").cast("string").alias("synced_source_url"),
          when(
            element.getField("last_synced_at").isNotNull,
            (element.getField("last_synced_at").cast("long") / lit(1000000L)).cast("timestamp")
          ).otherwise(null).alias("last_synced_at"),
          element.getField("is_origin").cast("boolean").alias("is_origin"),
          element.getField("sync_source_entity_type").cast("string").alias("sync_source_entity_type"),
          element.getField("additional_sync_info").cast("string").alias("additional_sync_info"),
          element.getField("is_deleted_from_sync_source").cast("boolean").alias("is_deleted_from_sync_source"),
          element.getField("last_import_hash").cast("long").alias("last_import_hash"),
          element.getField("sync_errors_info").cast("string").alias("sync_errors_info")
        )
      )

      val transformedDF = mongoDF
        .withColumn("_id", col("_id").cast("string"))
        .withColumn("created", (col("created") / lit(1000000L)).cast("timestamp"))
        .withColumn("updated", (col("updated") / lit(1000000L)).cast("timestamp"))
        .withColumn("deleted", col("deleted").cast("boolean"))
        .withColumn("name", col("name").cast("string"))
        .withColumn("billing_address", billingAddressCleaned)
        .withColumn("company_id", col("company_id").cast("string"))
        .withColumn("owner_user_id", col("owner_user_id").cast("string"))
        .withColumn("logo", col("logo").cast("string"))
        .withColumn("sync_data", syncDataCleaned)
        .withColumn("client_company_number", col("client_company_number").cast("string"))
        .withColumn("company_types", col("company_types").cast("array<string>"))
        .withColumn("market", col("market").cast("string"))
        .withColumn("campaign", col("campaign").cast("string"))
        .withColumn("client_company_identifier", col("client_company_identifier").cast("long"))
        .withColumn("previously_updated_at", (col("previously_updated_at") / lit(1000000L)).cast("timestamp"))
        .withColumn("fax", col("fax").cast("string"))
        .withColumn("website", col("website").cast("string"))
        .withColumn("linked_in_url", col("linked_in_url").cast("string"))
        .withColumn("facebook_url", col("facebook_url").cast("string"))
        .withColumn("twitter_url", col("twitter_url").cast("string"))
        .withColumn("source", col("source").cast("string"))
        .withColumn("date_acquired", col("date_acquired").cast("long"))
        .withColumn("shipping_addresses", col("shipping_addresses").cast("array<string>"))
        .withColumn("custom_fields", to_json(col("custom_fields")))
        .withColumn("tax_region_id", col("tax_region_id").cast("string"))
        .withColumn("read_only", col("read_only").cast("boolean"))
        .withColumn("archived", col("archived").cast("boolean"))
        .withColumn("blocked_for_import_at", col("blocked_for_import_at").cast("timestamp"))
        .withColumn("created_by_user_id", col("created_by_user_id").cast("string"))
        .withColumn("ms365_integration", col("ms365_integration").cast("string"))
        .withColumn("cloud_assessment_counters", to_json(col("cloud_assessment_counters")))
        .withColumn("is_dummy_data", col("is_dummy_data").cast("boolean"))

      spark.sql("CREATE DATABASE IF NOT EXISTS rough_db")
      spark.catalog.setCurrentDatabase("rough_db")
      transformedDF
        .limit(200)
        .write
        .mode("append")
        .format("parquet")
        .partitionBy("company_id")
        .saveAsTable("client_company_company_id_table")
      println("Written to Hive table rough_db.client_company_tbl in Parquet format")
    } finally {
      spark.stop()
    }
  }
}
