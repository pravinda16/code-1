package com.citi.sprinter.or2hive

import org.apache.spark.sql.hive.HiveContext
import com.citi.sprinter.core._
import com.citi.sprinter.util._

//  Author: Simon(jy44875) Time: 2017-05-16

object ORX {
  def fixCls(ora: ORC, tba:String): String = {

    val fmtDt = ora.x.sourceTableDateFormat
    val fmtTs = ora.x.sourceTableTimestampFormat
    
    val ncols = ora.orclcc.tbCols.map { c =>
      if(ora.orclcc.dtCols.contains(c) && !fmtDt.isEmpty ) {
        s"TO_CHAR(${tba}.${c}, '${fmtDt.get}') AS ${c}"

      } else if (ora.orclcc.tsCols.contains(c) && !fmtTs.isEmpty) {
        s"TO_CHAR(${tba}.${c}, '${fmtTs.get}') AS ${c}"   
      } else {
        s"${tba}.${c} AS ${c}"
      }
    }.mkString(",")
    
    LG.info(s"fmtDt: $fmtDt fmtTs: $fmtTs ")
    ncols
  }
  
  def newCls(x: SSXC): String = {
    val arr = x.sourceTableNewColumnsExpr.zip(x.sourceTableNewColumnsName).map(r => r._1 + " AS " + r._2)
    if (arr.length <= 0) "" else " , " + arr.mkString(" , ")
  }
  
  def newFilterCls(x: SSXC): String = {
    val arr = x.sourceTableNewColumnsExpr.zip(x.sourceTableNewColumnsName).map(r => r._1 + " = " + r._2)
     arr.mkString(" , ")
  } 
}