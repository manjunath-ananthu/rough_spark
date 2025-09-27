package com.roughspark

import org.apache.spark.sql.SparkSession

object ParquetSchemaPrinter {
  def main(args: Array[String]): Unit = {
    // By default read from the Hive table (metastore must be available)
    val defaultTable = "rough_db.client_company_id_table"
    val spark = SparkSession.builder()
      .appName("RoughSpark Parquet Schema Inspector")
      .enableHiveSupport()
      .getOrCreate()

    // If an argument is provided, treat it as a table name (no '/') or a parquet path (contains '/')
    val df = spark.table(defaultTable)
    // register a temp view for downstream inspection if needed
    df.createOrReplaceTempView("parquet_table")
    println("Showing 5 rows:")
    df.show(1, truncate = false)
    df.select("company_id").show(1)
    spark.stop()
  }
}
