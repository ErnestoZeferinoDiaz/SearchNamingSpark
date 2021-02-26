package com.zefe.searchnamings

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, regexp_replace, udf}
import org.apache.spark.sql.types.{DataType, StructType}

class SearchBuild {
  private var df:DataFrame = _
  private var sn = new SearchNamings
  private var commnads: List[CommandSearch] = List()
  private var listSuffixs: List[SuffixNaming.Value] = List()

  def show(noRows:Int=100,truncate: Boolean=true): Unit ={
    this.read()

    this.sn.setNamings(this.df)
    this.listSuffixs.foreach(println)
    this.commnads.foreach(com => com.execute())

    this.sn.show(noRows, truncate)
  }

  def andContaining(words: String*): SearchBuild ={
    this.commnads = this.commnads ::: List(new AndSearch(this.sn,words:_*))
    this
  }

  def andNotContaining(words: String*): SearchBuild ={
    this.commnads = this.commnads ::: List(new AndNotSearch(this.sn,words:_*))
    this
  }

  def filterBySuffix(suffixs: SuffixNaming.Value*): SearchBuild ={
    this.commnads = this.commnads ::: List(new FilterSearch(this.sn,suffixs:_*))
    this
  }

  def or(): SearchBuild ={
    if(!this.commnads.last.isInstanceOf[FilterSearch]){
      this.commnads = this.commnads ::: List(new FilterSearch(this.sn,SuffixNaming.All_WITH_OTHER))
    }
    this.commnads = this.commnads ::: List(new OrSearch(this.sn))
    this
  }

  private def read(): Unit ={
    var pathTmp:List[String] = List()
    this.listSuffixs = this.commnads.filter( c => {
      c.isInstanceOf[FilterSearch]
    }).map(
      c => c.asInstanceOf[FilterSearch]
    ).flatMap( f => {
      f.getSuffix
    }).distinct

    val isFileNamings = scala.reflect.io.File(Resource.pathNamingsOut).exists
    if (!isFileNamings){
      this.readXlsx()
      this.writeNamingParquet()
    }

    if(this.listSuffixs.contains(SuffixNaming.All_WITH_OTHER)){
      pathTmp = List(Resource.pathNamingsOut)
    }else if(this.listSuffixs.contains(SuffixNaming.ALL)){
      pathTmp = SuffixNaming.values.toList.filterNot( s => {
        s.equals(SuffixNaming.All_WITH_OTHER) ||
          s.equals(SuffixNaming.ALL) ||
          s.equals(SuffixNaming.OTHER)
      }).map( s => {
        Resource.pathNamingsOut+"/suffix="+s.toString
      })
    }else {
      pathTmp = this.listSuffixs.map( s => {
        Resource.pathNamingsOut+"/suffix="+s.toString
      })
    }

    pathTmp.foreach(println)
    this.df = Resource.spark.read.option("basePath",Resource.pathNamingsOut).parquet(pathTmp:_*)
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

  private def writeNamingParquet(): Unit ={
    this.df.write.partitionBy(
      "suffix"
    ).mode(
      SaveMode.Overwrite
    ).parquet(Resource.pathNamingsOut)
  }

}
