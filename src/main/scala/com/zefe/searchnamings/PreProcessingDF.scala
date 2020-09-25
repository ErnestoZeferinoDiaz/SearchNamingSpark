package com.zefe.searchnamings

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

class PreProcessingDF(spark: SparkSession, path: String) {

  def load(): DataFrame ={
    val df = spark.read
      .option("delimiter", "~")
      .option("header",true)
      .csv(path)

    val extractType: String => String = _.split("_").toList.last
    val extractTypeUDF = udf(extractType)

    df.select(
      col("FIELD CODE ID"),
      col("MEXICO - MARK OF USE"),
      extractTypeUDF(
        col("GLOBAL NAMING FIELD")
      ).as("Type Naming"),
      col("GLOBAL NAMING FIELD"),
      col("LOGICAL NAME OF THE FIELD (SPA)"),
      col("FIELD DESCRIPTION (SPA)")
    )
  }

}
