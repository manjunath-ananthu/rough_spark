package com.roughspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.{File, FileOutputStream}
import scala.io.Source

object SparkSimpleApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sparkC = new SparkContext(conf)
    val spark = SparkSession.builder().config(sparkC.getConf).getOrCreate()
    val resourceStream = getClass.getResourceAsStream("/README.md")
    val tempFile = File.createTempFile("tempReadMe", ".md", new File("/tmp"))
    tempFile.deleteOnExit()
    val outputStream = new FileOutputStream(tempFile)
    resourceStream.transferTo(outputStream)
    outputStream.close()
    resourceStream.close()
    println("++++++++++++++++++++++++")
    println(tempFile)
    println("++++++++++++++++++++++++")
    val df = spark.read.textFile(tempFile.getAbsolutePath)
    val numAs = df.filter(line => line.contains("a")).count()
    val numBs = df.filter(line => line.contains("b")).count()
    println("=------------------------------------")
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println("=------------------------------------")
    spark.stop()
  }
}
