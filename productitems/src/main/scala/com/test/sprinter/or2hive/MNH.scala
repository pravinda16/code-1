package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-01-27 ~ 2017-05-25

import com.citi.sprinter.core._
import com.citi.sprinter.util._
import org.apache.spark.sql.Row

class MNH(orcl: ORC) extends MORCL(orcl) {
  var cnt:Long=0
  
  def grs(): Array[Row] = {
    val qq = ORQ.getQ(db, tb, nspltx.intValue)
    val to = CUTL.getOption(orcl.s, qq, 1000)
    ssc.read.format("jdbc").options(to).load().collect()
  }
  
  def gc():String = {
    val where = if (fltexp.isEmpty) "" else s" WHERE ${fltexp.get}  "
    s"(SELECT /*+ CURSOR_SHARING_EXACT PARALLEL(8) */ COUNT(*) AS CNT FROM  $db.$tb T  $where ) y"
  }
  
  def gq(rs:Array[Row]): Array[(String, Int)] = {
    val rrr = rs.map( r =>( r.getString(1), r.getString(2) ) ).toArray
    val cond = if (fltexp.isEmpty) "" else s" AND ${fltexp.get}  "
    val xcols = ORX.fixCls(orcl, "A"); val ncols = ORX.newCls(orcl.x)
    
    val bbb = (0 to (rrr.length-1)).toArray
    val ids = TL.genid(bbb.length, (1 to sparkpd).toArray)
    val pks = TL.genpk(bbb.length, sparkpd); var k = -1
    
    val sqls = bbb.grouped(sparkpd).map { x =>
      val stat = x.map { y =>
        k=k+1; val id = ids(k); val pk = pks(k);      
        val r1 = rrr(k)._1; val r2 = rrr(k)._2; val rowcond = s" (ROWID BETWEEN CHARTOROWID('$r1') AND  CHARTOROWID('$r2'))"
        val q = s"SELECT T.*,               $id AS SPRINTER_ID,  '$pk' AS SPRINTER_PTK FROM  $db.$tb T  WHERE $rowcond $cond "
        q
      }
      val t1 = stat.mkString(" UNION ALL \n") 
      val rt = (s"( SELECT /*+ CURSOR_SHARING_EXACT */ $xcols $ncols, A.SPRINTER_ID,  A.SPRINTER_PTK FROM ( $t1 ) A ) y", x.length)
      rt
    }
   sqls.toArray
  }
     
  def gd(dci: Array[STCI]): String = {
    val cs = dci.filter( TL.fTbCols(_)(Array("SPRINTER_ID", "SPRINTER_PTK")) ).map{ r => 
      "%-30s %s".format( r.n, TL.fixAVRO(r.t, flfmt) )
    }.mkString(", \n")
    val tblfmt = TL.getTFD(flfmt)
    s"CREATE EXTERNAL TABLE $hivetb (\n$cs\n) PARTITIONED BY (SPRINTER_PTK STRING) $tblfmt LOCATION '$tbpath' "
  }
  
  def u(): Unit = {
    var kk = 0
    val rs = grs(); val qq = gq(rs)
    val sw = SW().start
    var nl : Array[String] = null

    cnt = if(!unnest) 0 else {
      val cntjo = CUTL.getOption(orcl.s, gc(), 1000); LG.logSparkOption(cntjo)
      val dfcnt = ssc.read.format("jdbc").options(cntjo).load()
      val tbcnt = dfcnt.first.getDecimal(0).longValue()
      sw.info("SPRINTER CNT  time:", s"total cnt: $tbcnt ")
      tbcnt
    }
    
    val total=qq.length
    qq.map { x =>
      kk     = kk + 1; val q = x._1;  val apd = x._2;
      sw.info("SPRINTER CNT  time:", s"total: $total, current: $kk ")
      val xo = CUTL.getXOption(apd, 1, apd + 1)
      val to = CUTL.getOption(orcl.s, q, jdbcfs)
      val jo = to ++ xo; LG.logSparkOption(jo)
      val df = ssc.read.format("jdbc").options(jo).load().cache()
      if (kk == 1) {         
        val tci = DS.getTCI(dsTCI, df, orcl)
        nl = tci.map(r=>r.n)

        val recon = DS.reconSrcWithDS(ssc, df, tci)
        if (!recon.succ) {
          if (!recon.inDSMT.isEmpty()) LG.error(recon.inDSMT)
          if (!recon.inHiveOrSrc.isEmpty()) LG.awpLog(recon.inHiveOrSrc)
        }
        ssc.sql(s"DROP TABLE IF EXISTS $hivetb"); val ddl = gd(tci); LG.debug(ddl);ssc.sql(ddl) //bugfix-24
        
        val dfh = ssc.sql(s"SELECT * FROM $hivetb LIMIT 1 "); val rrr = DS.reconTableSchema(df, dfh); LG.awpLog(rrr); dfh.unpersist()
        if( !rrr.isEmpty() ) { LG.error(rrr) }   
      }
      
      val np=s"$tbpath/sprinter_ptk=$kk"
      DF.save(df, np, flfmt, nl); sw.debug("SPRINTER PTK  time:", s"path: $np"); df.unpersist()
      if(unnest) {
        LG.awpFileIds( HV.getHdfsFiles(ssc, np));
      }
    }
    
    ssc.sql(s"MSCK REPAIR TABLE $hivetb")
    if(unnest) LG.awpStat(s"$cnt")
  }

  def r(): Unit = {
    LG.info("In MNH")
    var bkl: String = ""; val bkf = unnest && HV.tbNotEmpty(ssc, hivetb)
    if(bkf) { var bkf = HV.bkETTable(ssc, hivetb); if( bkf.isEmpty) LG.error(s"Failed to backup table: $hivetb"); bkl=bkf.get } 
    try {
      u()
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