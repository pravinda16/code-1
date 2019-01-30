package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2018-02-04 ~ 2018-03-09

import com.citi.sprinter.core._
import com.citi.sprinter.util._

class MPDDrv(orcl: ORC) extends MORCL(orcl) {
  val allPtkl = orcl.s.targetTablePartitionColumn.get.toLowerCase
  val allPtku = orcl.s.targetTablePartitionColumn.get.toUpperCase
  val aptkl   = allPtkl.split(",").map( r => r.trim() ) 
  val aptku   = allPtku.split(",").map( r => r.trim() ) 

  //2017-07-02
  val _dtIdx  = orcl.s.targetTablePartitionDateColumnIndex
  val dtIdx   = if( _dtIdx < 0 || _dtIdx > aptkl.length ) aptkl.length - 1 else _dtIdx - 1
  val tptkl   = aptkl( dtIdx )
  val tptku   = aptku( dtIdx )
  
def fixDerivedCols(valueType: String): String = {
    val tmpsx = aptku.map { k =>
      val tmpIndex = orcl.x.sourceTableNewColumnsName.indexOf(k)
      if( tmpIndex < 0 ) k else {
        val tmpExprV = orcl.x.sourceTableNewColumnsExpr(tmpIndex)
        if( valueType == "Name") s"$tmpExprV AS $k" else s"$tmpExprV "
      }
    }
    tmpsx.mkString(" , ")
  }
  
  def r(): Unit = {
    LG.info("In MPDDrv"); val sw = SW().start
    
    val where = if( fltexp.isEmpty) "" else s" WHERE ${fltexp.get}"
    val colsx = fixDerivedCols("Name"); val colsy = fixDerivedCols("Value")
    val sqlora = s"""( SELECT $colsx,  TO_CHAR( COUNT(*) ) AS CNT FROM $db.$tb $where GROUP BY $colsy ) y"""
    val optora = CUTL.getOption(orcl.s, sqlora, 1000); LG.debug(sqlora)
    val dfora  = ssc.read.format("jdbc").options(optora).load().cache()
    dfora.registerTempTable("oratab123"); val mindt  = ssc.sql(s"SELECT MIN($tptku) FROM oratab123").first().getString(0)
    LG.info(s"mindt: $mindt")
    
    val sqlhiv = s"SELECT  $allPtku, CAST( COUNT(*) AS STRING) AS CNT FROM $hivetb WHERE $tptku >= '$mindt' GROUP BY $allPtku"
    val dfhiv  = ssc.sql(sqlhiv).cache(); LG.debug(sqlhiv)
    val dfnew = dfora.except(dfhiv); dfnew.cache(); dfnew.printSchema(); dfnew.show(10000,false)
    val dstct = dfnew.count(); sw.debug(s"SPRINTER CNT  time: dstct: $dstct")
    val rows  = dfnew.collect(); dfhiv.unpersist(); dfora.unpersist(); dfnew.unpersist()
    
    if( dstct <= 0 ) {
      LG.awpLog("NO NEW DATA")
      cleanExpiredData(tptkl)
    } else {      
      val fExpr = rows.map { row =>
        val rval=(0 to (aptku.length-2) ).map{ i => 
          val tmpv = row.getString(i)
          val tmpn = aptku(i)
          s"$tmpn = '$tmpv' "
        } 
        val dtv = row.getString(aptku.length-1)
        val dte1 = rval.mkString(" AND ")
        val dte2 = s"$tptku = TO_DATE('$dtv', 'YYYY-MM-DD')" 
        if  (dte1.isEmpty() ) dte2 else s"( $dte1  AND  $dte2 )"
      }.mkString(" OR \n")
      val nw = if( dstct < 186 ) s"( $fExpr )" else ""; LG.info(s"LAST UPDATE TIME, FILTER EXPRESS: $nw")
      val nx = CUTL.dumpSSXC(orcl.x, nw)
      val norcl = ORC(orcl.s , nx, orcl.r, dumpOrcl(true, true))
      val m = new MPCore(norcl)
      m.r()
    }
  }
}