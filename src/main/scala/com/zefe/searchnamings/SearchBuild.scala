package com.zefe.searchnamings

import java.text.Normalizer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lower, udf, upper}

class SearchBuild {
  private var dFOrigin:DataFrame = _
  private var dFTmp:DataFrame = _
  private var dFResult:DataFrame = _

  private var cleanStringUDF:UserDefinedFunction =_
  private val columnsSearch = List[String](
    "field_code_id",
    "mexico_mark_of_use",
    "global_naming_field",
    "logical_name_of_the_field_spa",
    "field_description_spa"
  )

  def init(): SearchBuild ={
    this.dFOrigin = Resource.spark.read.parquet(Resource.pathNamingsOut)
    this.dFTmp = this.dFOrigin

    this.cleanStringUDF = udf( (palabra:String) => {
      val cadena: String = palabra.trim.toLowerCase()
      val cadenaNormalize: String = Normalizer.normalize(cadena, Normalizer.Form.NFD)
      cadenaNormalize.replaceAll("[^\\p{ASCII}]", "")
    })

    this
  }

  def andContaining(words: String*): SearchBuild ={
    val tmp = words.map( x => this.cleanString(x)).toList.mkString("|")

    val condition = this.columnsSearch.map( column => {
      cleanStringUDF(col(column)).rlike(tmp)
    }).reduce(_||_)

    this.dFTmp = this.dFTmp.filter(condition)

    this
  }

  def andNotContaining(words: String*): SearchBuild ={

    this
  }

  def or(): SearchBuild ={
    this
  }

  def result():DataFrame={
    this.dFTmp.select(
      col("field_code_id"),
      col("mexico_mark_of_use"),
      col("global_naming_field"),
      col("logical_name_of_the_field_spa"),
      col("field_description_spa")
    )
  }

  private def cleanString(palabra: String): String ={
    val cadena: String = palabra.trim.toLowerCase()
    val cadenaNormalize: String = Normalizer.normalize(cadena, Normalizer.Form.NFD)
    cadenaNormalize.replaceAll("[^\\p{ASCII}]", "")
  }
}




object SuffixNaming extends Enumeration{
  val ID = Value("id")
  val TYPE = Value("type")
  val AMOUNT = Value("amount")
  val NUMBER = Value("number")

  val PER = Value("per")
  val PERCENTAGE = Value("per")

  val DATE = Value("date")
  val TIME_DATE = Value("time_date")
  val HMS_DATE = Value("hms_date")

  val DESC = Value("desc")
  val NAME = Value("name")

  val ALL = Value("*")
}