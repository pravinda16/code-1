package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-01-24 ~ 2017-05-27

import com.citi.sprinter.core._
import com.citi.sprinter.util._

class MNS(orcl: ORC) extends MORCL(orcl) {
  var cnt:Long=0
  
  def gc():String = {
    val where = if (fltexp.isEmpty) "" else s" WHERE ${fltexp.get}  "
    s"(SELECT /*+ CURSOR_SHARING_EXACT PARALLEL(8) */ COUNT(*) AS CNT FROM  $db.$tb T  $where ) y"
  }
  
  def gq(): String = {
    val where = if (fltexp.isEmpty) "" else s" WHERE ${fltexp.get}"
    val cols = ORX.fixCls(orcl, "A"); val ncls = ORX.newCls(orcl.x)
    s"""( SELECT /*+ CURSOR_SHARING_EXACT */ $cols, ORA_HASH(ROWID, $sparkpd)+1 AS SPRINTER_ID $ncls FROM $db.$tb A $where ) y"""   
  }
    
  def gd(dci: Array[STCI]): String = {
    val cs = dci.filter( TL.fTbCol(_)() ).map{ r => 
      "%-30s %s".format( r.n, TL.fixAVRO(r.t, flfmt) )
    }.mkString(", \n")
    val tblfmt = TL.getTFD(flfmt)
    s"CREATE EXTERNAL TABLE $hivetb (\n$cs\n) $tblfmt LOCATION '$tbpath' "
  }
  
  def r(): Unit = {
    LG.info("In MNS")
    val sw = SW().start
    
    cnt = if(!unnest) 0 else {
      val cntjo = CUTL.getOption(orcl.s, gc(), 1000); LG.logSparkOption(cntjo)
      val dfcnt = ssc.read.format("jdbc").options(cntjo).load()
      val tbcnt = dfcnt.first.getDecimal(0).longValue()
      sw.info("SPRINTER CNT  time:", s"total cnt: $tbcnt ")
      tbcnt
    }
    
    val xo = CUTL.getXOption(sparkpd, 1, sparkpd + 1)
    val to = CUTL.getOption(orcl.s, gq(), jdbcfs)
    val jo = to ++ xo; LG.logSparkOption(jo)
    
    val df  = ssc.read.format("jdbc").options(jo).load().cache()
    val tci = DS.getTCI(dsTCI, df, orcl)
    
    var bkl:String=""; val bkf = unnest && HV.tbNotEmpty(ssc, hivetb)
    if(bkf) { var bkf = HV.bkETTable(ssc, hivetb); if( bkf.isEmpty) LG.error(s"Failed to backup table: $hivetb"); bkl=bkf.get }  
    
    val recon = DS.reconSrcWithDS(ssc, df, tci)
    if (!recon.succ) {
      if (!recon.inDSMT.isEmpty())      LG.error(recon.inDSMT)
      if (!recon.inHiveOrSrc.isEmpty()) LG.awpLog(recon.inHiveOrSrc)
    }
    ssc.sql(s"DROP TABLE IF EXISTS $hivetb");val ddl = gd(tci); LG.debug(ddl); ssc.sql(ddl) //bugfix-24
    
    val dfh = ssc.sql(s"SELECT * FROM $hivetb LIMIT 1 "); val rrr = DS.reconTableSchema(df, dfh); LG.awpLog(rrr); dfh.unpersist()
    if( !rrr.isEmpty() ) { LG.error(rrr) }
    
    try {
      DF.save(df, tbpath, flfmt, tci.map(r => r.n)); df.unpersist()
      if(unnest) {
        LG.awpStat(s"$cnt"); LG.awpFileIds( HV.getHdfsFiles(ssc, tbpath));
      }
    } catch {
      case ex: Exception => { if(bkf) HV.rsETTable(ssc, hivetb, bkl); throw ex }
    }
    
    //keep data for filter
    if(bkf) {
      if( cnt == 0 && !fltexp.isEmpty ) {
        HV.rsETTable(ssc, hivetb, bkl)
      } else {
        HV.rmETTableBk(ssc, bkl, hivetb)
      }
    }
  }  
}