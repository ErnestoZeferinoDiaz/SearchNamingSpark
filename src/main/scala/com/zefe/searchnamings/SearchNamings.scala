package com.zefe.searchnamings

import java.text.Normalizer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, length, lower, udf, upper}

class SearchNamings(){
  private var dFOrigin:DataFrame = _
  private var dFTmp:List[DataFrame] = List[DataFrame]()
  private var dFResult:DataFrame = _

  private var cleanStringUDF:UserDefinedFunction =_
  private val columnsSearch = List[String](
    "field_code_id",
    "mexico_mark_of_use",
    "global_naming_field",
    "logical_name_of_the_field_spa",
    "field_description_spa"
  )



  def setNamings(df: DataFrame): SearchNamings ={
    this.dFOrigin = df
    this.dFTmp = this.dFOrigin :: this.dFTmp

    this.cleanStringUDF = udf( (palabra:String) => {
      val cadena: String = palabra.trim.toLowerCase()
      val cadenaNormalize: String = Normalizer.normalize(cadena, Normalizer.Form.NFD)
      cadenaNormalize.replaceAll("[^\\p{ASCII}]", "")
    })

    this
  }

  def andContaining(words: String*): SearchNamings ={
    val tmp = words.map( x => this.cleanString(x)).toList.mkString("|")
    val df = this.dFTmp(0)
    val condition = this.columnsSearch.map( column => {
      cleanStringUDF(col(column)).rlike(tmp)
    }).reduce(_||_)
    this.dFTmp = df.filter(condition) :: this.dFTmp.slice(1,this.dFTmp.length)
    this
  }

  def andNotContaining(words: String*): SearchNamings ={
    val tmp = words.map( x => this.cleanString(x)).toList.mkString("|")
    val df = this.dFTmp(0)
    val condition = this.columnsSearch.map( column => {
      cleanStringUDF(col(column)).rlike(tmp)
    }).reduce(_||_)
    this.dFTmp = df.filter(!condition) :: this.dFTmp.slice(1,this.dFTmp.length)
    this
  }

  def filterBySuffix(suffixs: SuffixNaming.Value*): SearchNamings ={
    var df = this.dFTmp(0)
    var fil:List[SuffixNaming.Value] = List(
      SuffixNaming.TIME_DATE,
      SuffixNaming.DATE,
      SuffixNaming.HMS_DATE,
      SuffixNaming.ID,
      SuffixNaming.NAME,
      SuffixNaming.AMOUNT,
      SuffixNaming.DESC,
      SuffixNaming.NUMBER,
      SuffixNaming.PER,
      SuffixNaming.TYPE
    )

    if(suffixs.contains(SuffixNaming.All_WITH_OTHER)){
      fil = List()
    }else if(suffixs.contains(SuffixNaming.ALL)){

    }else{
      fil = suffixs.toList
    }

    df = fil.map( f => {
      df.filter(col("suffix")===f.toString)
    }).reduceLeft(_.union(_))


    this.dFTmp = df :: this.dFTmp.slice(1,this.dFTmp.length)
    this
  }

  def or(): SearchNamings ={
    this.dFTmp = this.dFOrigin :: this.dFTmp
    this
  }

  def show(noRows:Int=100,truncate: Boolean=true): Unit ={
    this.dFTmp.reverse.foreach( df => {
      df.orderBy(
        col("mexico_mark_of_use").desc,
        length(col("logical_name_of_the_field_spa")).asc,
        //col("type_naming").asc,
        col("global_naming_field").asc,
        col("logical_name_code").asc

      ).select(
        col("field_code_id"),
        col("mexico_mark_of_use"),
        col("suffix"),
        col("global_naming_field"),
        col("logical_name_of_the_field_spa"),
        col("field_description_spa")
      ).show(noRows,truncate)
    })
  }

  private def cleanString(palabra: String): String ={
    val cadena: String = palabra.trim.toLowerCase()
    val cadenaNormalize: String = Normalizer.normalize(cadena, Normalizer.Form.NFD)
    cadenaNormalize.replaceAll("[^\\p{ASCII}]", "")
  }
}

abstract class CommandSearch(sn: SearchNamings){
  def execute()
}

class OrSearch(sn: SearchNamings) extends CommandSearch(sn){
  def execute(): Unit ={
    this.sn.or()
  }
}

class AndSearch(sn: SearchNamings,words: String*) extends CommandSearch(sn){
  def execute(): Unit ={
    this.sn.andContaining(this.words:_*)
  }
}

class AndNotSearch(sn: SearchNamings,words: String*) extends CommandSearch(sn){
  def execute(): Unit ={
    this.sn.andNotContaining(this.words:_*)
  }
}

class FilterSearch(sn: SearchNamings,suf: SuffixNaming.Value*) extends CommandSearch(sn){
  def execute(): Unit ={
    this.sn.filterBySuffix(this.suf:_*)
  }

  def getSuffix: List[SuffixNaming.Value] ={
    this.suf.toList
  }
}

object SuffixNaming extends Enumeration{
  val ID = Value("id")
  val TYPE = Value("type")
  val AMOUNT = Value("amount")
  val NUMBER = Value("number")

  val PER = Value("per")

  val DATE = Value("date")
  val TIME_DATE = Value("time_date")
  val HMS_DATE = Value("hms_date")

  val DESC = Value("desc")
  val NAME = Value("name")

  val OTHER = Value("other")

  val ALL = Value("*")
  val All_WITH_OTHER = Value("*")
}