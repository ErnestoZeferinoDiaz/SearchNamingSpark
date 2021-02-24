package com.zefe.searchnamings


/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {

    val rNDF = new ReadNamings
    val df = rNDF.read.compact.getNamingsDF

    val stm = new SearchBuild()

    stm.init(
    ).andContaining(
      "fecha","hora"
    ).andContaining(
      "operacion","movimiento","operativa"
    ).result().show(1000,false)






  }

}
