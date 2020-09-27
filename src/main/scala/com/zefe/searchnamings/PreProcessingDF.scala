package com.zefe.searchnamings

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf, length}
import org.apache.spark.sql.types.{DataType, StructType}

class PreProcessingDF(res:Resource, csv: List[List[String]]) {

  def load(): DataFrame ={
    val schemaSource = scala.io.Source.fromFile(res.pathSchemaAll).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    val data = this.csv.slice(1,this.csv.length).map(Row.fromSeq(_))
    val df = res.spark.createDataFrame(res.sc.parallelize(data),schemaFromJson)

    val extractType: String => String = _.split("_").toList.last
    val extractTypeUDF = udf(extractType)

    val dfMin = df.select(
      col("field_code_id"),
      col("mexico_mark_of_use"),
      extractTypeUDF(
        col("global_naming_field")
      ).as("type_naming"),
      col("global_naming_field"),
      col("logical_name_of_the_field_spa"),
      col("field_description_spa")
    )

    dfMin
  }

}
