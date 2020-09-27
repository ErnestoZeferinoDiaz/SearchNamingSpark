package com.zefe.searchnamings

import java.text.Normalizer

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class SearchNaming(res:Resource){
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

  def isWordInPrayer(word: String, prayer: String): Boolean ={
    prayer.split("[\\s\\n\\t]+").toList.map( x => {
      cleanString(x).toLowerCase
    }).map(x => {
      val r = x.matches("(?i)("+word+").{0,4}$")
      r
    }).reduce(_|_)

  }

  def isSomeWordInPrayer(prayer: String, words: List[String]): Boolean ={
    words.map( word => {
      isWordInPrayer(word,prayer)
    }).reduce(_|_)
  }

  def isAllWordInPrayer(prayer: String): Boolean ={
    this.words.map( word => {
      isSomeWordInPrayer(prayer,word)
    }).foldLeft(true)(_&_)
  }

  def search(): Dataset[Row] ={
    import this.res.spark.sqlContext.implicits._

    val df = res.spark.read.parquet(res.pathNamings2).select(
      col("field_code_id"),
      col("mexico_mark_of_use"),
      col("type_naming"),
      col("global_naming_field"),
      col("logical_name_of_the_field_spa"),
      col("field_description_spa")
    )
    val dfC = df.collect().map(x => {
      x.toSeq.toList.map(y => y.toString)
    }).toList

    val resp = dfC.filter(row => {
      row.map(prayer => {
        val r = isAllWordInPrayer(prayer)
        r
      }).fold(false)(_|_)
    }).map(x => {
      Row.fromSeq(x)
    })
    val rdd = this.res.sc.parallelize(resp)
    val schema = df.schema
    this.res.spark.createDataFrame(rdd,schema)
  }
}
