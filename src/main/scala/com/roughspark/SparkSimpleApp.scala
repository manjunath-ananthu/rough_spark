package com.roughspark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSimpleApp {
  def main(args: Array[String]): Unit = {
  }

  def readingJsonFile(sparkSession: SparkSession) = {
    val df = sparkSession.read.format("json").load("../../spark_examples/data/flight-data/json/2015-summary.json")
    df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").limit(2)
    println("==================Result======================")
    println("===============================================")
  }

}
