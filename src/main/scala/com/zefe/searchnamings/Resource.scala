package com.zefe.searchnamings

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Resource {
  val pathSchema = "src/main/resources/schemaTableNaming.json"
  val pathSchemaWithSuffix = "src/main/resources/schemaTableNamingWithSuffix.json"

  val pathNamingsIn = "src/main/resources/DDNG-N-8634.xlsx"
  val pathNamingsOut = "src/main/resources/namings_2"


  val conf = new SparkConf().setAppName("hola").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
}
