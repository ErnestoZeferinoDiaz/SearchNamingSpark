package com.zefe.searchnamings

import org.apache.spark.sql.functions.{col, regexp_replace, udf}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{DataType, StructType}

class ReadNamings{
  private var df:DataFrame = _

  def read(): ReadNamings = {
    val tmp = scala.reflect.io.File(Resource.pathNamingsOut).exists

    if (!tmp){
      this.readXlsx()
      this.writeNamingParquet()
    }

    this
  }

  def compact(): ReadNamings ={

    this.df = this.df.select(
      col("field_code_id"),
      col("mexico_mark_of_use"),
      col("peru_mark_of_use"),
      col("global_naming_field"),
      col("suffix"),
      col("logical_name_of_the_field_spa"),
      col("field_description_spa")
    )
    this
  }

  def getNamingsDF:DataFrame=this.df

  private def writeNamingParquet(): Unit ={
    this.df.write.partitionBy(
      "mexico_mark_of_use"
    ).mode(
      SaveMode.Overwrite
    ).parquet(Resource.pathNamingsOut)
  }

  private def readXlsx(): Unit ={
    val schemaSource = scala.io.Source.fromFile(Resource.pathSchema).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    this.df = Resource.spark.read
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

    val columns = this.df.columns
    val condition = columns.map( column => {
      regexp_replace(col(column), "\n|\r", "").alias(column)
    })

    this.df = this.df.select(condition:_*)
    val sufix = udf( (naming:String) => {
      var suf = List(
        "id" -> "id",
        "type" -> "type",
        "amount" -> "amount",
        "number" -> "number",
        "per" -> "per",
        "date" -> "date",
        "time_date" -> "time_date",
        "hms_date" -> "hms_date",
        "desc" -> "desc",
        "name" -> "name"
      ).toMap
      var tmp = naming.split("_").toList
      var list = tmp.slice(tmp.length-2,tmp.length)
      var dos = list.mkString("_")
      var uno = list.last
      var resp = ""

      if(suf.contains(dos)){
        resp = suf(dos)
      }else if(suf.contains(uno)){
        resp = suf(uno)
      }else{
        resp = "other"
      }
      resp
    })

    this.df = this.df.withColumn(
      "suffix",sufix(col("global_naming_field"))
    ).select(
      col("*")
    )
  }
}

