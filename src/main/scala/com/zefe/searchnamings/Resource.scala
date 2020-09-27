package com.zefe.searchnamings

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class Resource {
  val pathSchemaMin = "src/main/resources/schemaMin.json"
  val pathSchemaAll = "src/main/resources/schemaAll.json"
  val pathNamings = "src/main/resources/namings.csv"
  val pathNamings2 = "src/main/resources/namings_2"
  val conf = new SparkConf().setAppName("hola").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
}
