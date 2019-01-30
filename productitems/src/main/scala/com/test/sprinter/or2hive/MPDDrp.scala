package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-04-28

import com.citi.sprinter.core._
import com.citi.sprinter.util._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame

class MPDDrp(orcl: ORC) extends MORCL(orcl) {
  def r(): Unit = {
    LG.info("In MPDTrunc")
    var bkl: String = ""; val bkf = true
    if(bkf) { var bkf = HV.bkETTable(ssc, hivetb); if( bkf.isEmpty) throw new SSE(s"Failed to backup table: $hivetb"); bkl=bkf.get } 
    try {
      val norcl = ORC(orcl.s , orcl.x, orcl.r, dumpOrcl(false, true)); val m = new MPCore(norcl); m.r()
    } catch {
      case ex: Exception => { if(bkf) HV.rsETTable(ssc, hivetb, bkl); throw ex }
    }
    if(bkf) HV.rmETTableBk(ssc, bkl, hivetb)    
  }
}