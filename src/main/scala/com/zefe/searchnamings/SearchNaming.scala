package com.zefe.searchnamings

import java.text.Normalizer

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lower, udf, upper}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

class SearchNaming(){
  private var words:List[List[String]] = List[List[String]]()

  def words(palabras: String): SearchNaming ={
    words = palabras.split(",").toList :: words
    this
  }



  def cleanString(palabra: String): String ={
    val cadena: String = palabra.trim
    val cadenaNormalize: String = Normalizer.normalize(cadena, Normalizer.Form.NFD)
    cadenaNormalize.replaceAll("[^\\p{ASCII}]", "")
  }

  def isSomeWordsInColumn(words:List[String], colName: String): Column ={
    lower(
      col(colName)
    ).rlike(
      words.map(_.toLowerCase).mkString("|")
    )
  }

  def isAllWordsInColumn(colName: String): Column ={
    this.words.map( word => {
      isSomeWordsInColumn(word,colName)
    }).reduce(_&&_)
  }

  def isAllWordsInAllCols(cols: List[String]): Column ={
    cols.map(column => {
      isAllWordsInColumn(column)
    }).reduce(_||_)
  }

  def search(): Dataset[Row] ={
    val df = Resource.spark.read.parquet(Resource.pathNamingsOut).select(
      col("field_code_id"),
      col("mexico_mark_of_use"),
      col("type_naming"),
      col("global_naming_field"),
      col("logical_name_of_the_field_spa"),
      col("field_description_spa")
    )
    val cols = df.columns.toList
    val condition = isAllWordsInAllCols(cols)

    println(condition)
    df.select("*").where(condition)
  }


}
