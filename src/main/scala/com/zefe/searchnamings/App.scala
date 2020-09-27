package com.zefe.searchnamings

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object App {



  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hola").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()
    import spark.sqlContext.implicits._

    //val csv = new PreProcessingCSV("src/main/resources/namings.csv").load()
    val df = new PreProcessingDF(spark,"src/main/resources/namings_2.csv").load()

    /*
    val searchNaming = new SearchNaming(
      sc,spark,df
    )
      .words("saldo")
      .words("retenido")

    .search.orderBy(
      col("MEXICO - MARK OF USE").desc,
      col("Type Naming").asc,
      length(col("LOGICAL NAME OF THE FIELD (SPA)")).asc,
      col("GLOBAL NAMING FIELD").asc
    ).show(500,false)

     */
  }

}
