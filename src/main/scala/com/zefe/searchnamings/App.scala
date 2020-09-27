package com.zefe.searchnamings

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {
    val res = new Resource()

    //procesingExcel(res)



    val searchNaming = new SearchNaming(res)
      .words("fecha")
      .words("alta")
    .search.orderBy(
      col("mexico_mark_of_use").desc,
      col("type_naming").asc,
      length(col("logical_name_of_the_field_spa")).asc,
      col("global_naming_field").asc
    ).show(100,false)


  }

  def procesingExcel(res: Resource): Unit ={
    val csv = new PreProcessingCSV(res).load()
    val df = new PreProcessingDF(res,csv).load()
    df.write.partitionBy(
      "mexico_mark_of_use"
    ).mode(
      SaveMode.Overwrite
    ).parquet(res.pathNamings2)
  }

}
