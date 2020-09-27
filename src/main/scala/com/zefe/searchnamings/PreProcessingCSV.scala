package com.zefe.searchnamings

import java.io.{BufferedWriter, FileWriter}

class PreProcessingCSV(path: String) {
  val emailRegex = "^[\\w-_\\.+]*[\\w-_\\.]\\@([\\w]+\\.)+[\\w]+[\\w]$"
  val delimiter = "~"
  val noColsOfExcel = 56

  private def read(mpath: String): List[String] ={
    scala.io.Source.fromFile(this.path)
      .getLines()
      .toList
  }

  private def writeNewCSV(headers: List[String], data: List[List[String]]): Unit ={
    val resp = headers :: data
    val newname = path.split("/").toList.last.split("\\.").toList(0)+"_2.csv"
    val nameOutput = path.split("/").toList.filter(!_.matches(".*\\.csv")).mkString("/") + "/"+newname
    val writer = new BufferedWriter(new FileWriter(nameOutput))
    var y = 0;
    resp.map( x => {
      x.mkString(this.delimiter) + "\n"
    }).foreach(writer.write)
    writer.close()
  }

  private def addDelimiterAfterEmail(x: String): String={
    var lista = x.split(this.delimiter).toList
    var ult = lista.last
    ult = ult.slice(1,ult.length-1)
    var valor = ult.matches(this.emailRegex)
    if(valor){
      x + this.delimiter
    }else{
      x
    }
  }


  def load(): List[List[String]] ={
    val tmp = read(this.path)
    val headers = tmp.slice(0,1).mkString.split(this.delimiter).toList

    val file = tmp.slice(1,tmp.length).map(x => {
      addDelimiterAfterEmail(x)
    }).mkString.split(this.delimiter).toList.grouped(this.noColsOfExcel).toList

    writeNewCSV(headers,file)

    headers::file
  }
}
