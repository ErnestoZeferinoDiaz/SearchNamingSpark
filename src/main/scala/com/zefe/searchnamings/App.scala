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

    val schemaSource = scala.io.Source.fromFile(res.pathSchemaAll).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    val df = res.spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "DDNG-N") // Required
      .option("header", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "true") // Optional, default: false
      .option("startColumn", 0) // Optional, default: 0
      .option("endColumn", 56) // Optional, default: Int.MaxValue
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10)
      .schema(schemaFromJson)
      .load("src/main/resources/DDNG-N-8634.xlsx")

    df.show()
    println(df.count())

    df.write.partitionBy(
      "mexico_mark_of_use"
    ).mode(
      SaveMode.Overwrite
    ).parquet(res.pathNamings2)







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
