package com.zefe.searchnamings

import org.apache.spark.sql.functions.col


/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {
    val stm = new SearchBuild()

    stm.andContaining(
      "fecha","hora"
    ).andContaining(
      "operacion","movimiento","operativa"
    ).andContaining(
      "local"
    ).andNotContaining(
      "divisa","moneda"
    ).andNotContaining(
      "texto"
    ).filterBySuffix(
      SuffixNaming.ID
    ).or(

    ).andContaining(
      "branch[0-9]*"
    ).filterBySuffix(
      SuffixNaming.DATE
    ).show(100,false)





  }

}
