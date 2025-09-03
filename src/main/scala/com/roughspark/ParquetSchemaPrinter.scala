package com.roughspark

import org.apache.spark.sql.SparkSession

object ParquetSchemaPrinter {
  def main(args: Array[String]): Unit = {
    // By default read from the Hive table (metastore must be available)
    val defaultTable = "rough_db.client_company_tbl"
    val spark = SparkSession.builder()
      .appName("RoughSpark Parquet Schema Inspector")
      .enableHiveSupport()
      .getOrCreate()

    // If an argument is provided, treat it as a table name (no '/') or a parquet path (contains '/')
    val df = spark.table(defaultTable)
    df.printSchema()
    // register a temp view for downstream inspection if needed
    df.createOrReplaceTempView("parquet_table")
    println("Showing 5 rows:")
    df.show(5, truncate = false)

    spark.stop()
  }
}
