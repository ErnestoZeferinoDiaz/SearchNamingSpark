package com.zefe.searchnamings

import java.io.{BufferedWriter, FileWriter}

class PreProcessingCSV(path: String) {

  def load(): List[List[String]] ={
    var emailRegex = "^[\\w-_\\.+]*[\\w-_\\.]\\@([\\w]+\\.)+[\\w]+[\\w]$"
    var x = 0
    val tmp = scala.io.Source.fromFile(this.path)
      .getLines()
      .toList

    val headers = tmp.slice(0,1).mkString.split("~").toList

    val file = tmp.slice(1,tmp.length).map(x => {
      var lista = x.split("~").toList
      var ult = lista.last
      ult = ult.slice(1,ult.length-1)
      var valor = ult.matches(emailRegex)
      if(valor){
        x + "~"
      }else{
        x
      }
    }).mkString.split("~").toList.grouped(56).toList
    val resp = headers :: file

    val newname = path.split("/").toList.last.split("\\.").toList(0)+"_2.csv"
    val nameOutput = path.split("/").toList.filter(!_.matches(".*\\.csv")).mkString("/") + "/"+newname

    println(nameOutput)
    val writer = new BufferedWriter(new FileWriter(nameOutput))
    var y = 0;

    resp.map( x => {
      x.mkString("~") + "\n"
    }).foreach(writer.write)
    writer.close()
    resp
  }
}
