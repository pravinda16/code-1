package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-01-24 ~ 2017-05-16

import com.citi.sprinter.core._
import com.citi.sprinter.util._
import java.text.SimpleDateFormat
import java.util.Calendar

abstract class MORCL(orclc: ORC) extends TETLM {
  val ssc    = orclc.r.sparkSSC
  val db     = orclc.s.sourceTable.split("\\.")(0).toUpperCase()
  val tb     = orclc.s.sourceTable.split("\\.")(1).toUpperCase()
  val fltexp = orclc.x.sourceTableFilterExpr
  val hivedb = orclc.s.targetTable.split("\\.")(0).toLowerCase()
  val hivetb = orclc.s.targetTable.split("\\.")(1).toLowerCase()
  val dbpath = orclc.s.targetTableLocation.getOrElse( HV.getDBFolder(ssc, hivedb) ) 
  val tbpath = if( dbpath.endsWith("/")) s"${dbpath}${hivetb}" else s"${dbpath}/${hivetb}"
  val maxpd  = orclc.r.sparkMaxParallelDegree
  val ideapd = orclc.r.ideaParallelDegree
  val sparkpd= orclc.r.sparkParallelDegree.toInt
  val nspltx = (orclc.orclcc.tabMB >> orclc.orclcc.blkNN)/8*8
  val flfmt  = orclc.s.targetTableFileFormat
  val jdbcfs = orclc.r.sourceTableJDBCFetchSize
  val retenp = orclc.s.targetTablePartitionRetentionPolicy
  val unnest  = orclc.orclcc.depth == 0
  val flBkF   = orclc.orclcc.flBkF
  val dsTCI   = DS.getTCI(orclc)
  val nodataE =  if( !orclc.x.sourceTableDisableNoDataException.isEmpty && orclc.x.sourceTableDisableNoDataException.get == "ON") false else true
  
  def dumpOrcl(inf:Boolean, flbak:Boolean): ORCC = {
    val nbakf=if(inf) orclc.orclcc.flBkF else flbak
    ORCC( orclc.orclcc.tabBK, orclc.orclcc.tabMB, orclc.orclcc.cpuNN, orclc.orclcc.blkNN, orclc.orclcc.vwTabs, orclc.orclcc.tbCols, orclc.orclcc.dtCols, orclc.orclcc.tsCols, orclc.orclcc.depth + 1, nbakf)
  }
  
  def getMinYMD(): String = {
      val fmtYMD = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance(); cal.setTime(fmtYMD.parse(orclc.s.sprinterAsOfDate)); cal.add(Calendar.DATE, -1 * (retenp-1).intValue)
      val mindt=cal.getTime;val minYMD=fmtYMD.format(mindt)
      minYMD
  }
  
  def cleanExpiredData(ptkl: String) = {
    if(!flBkF && retenp > 0L ) {
      val fmtYMD = new SimpleDateFormat("yyyy-MM-dd"); val minYMD = getMinYMD(); val mindt  = fmtYMD.parse(minYMD)
      val fixdates  = HV.getExpiredDate(ssc, hivetb, ptkl, minYMD)  //01    
      val dfprts = ssc.sql(s"SHOW PARTITIONS $hivetb").collect()
      
      //clean up
      dfprts.map{ row =>
        val pts=row.get(0).toString(); val ptvYMD=HT.getYMDFromPTS(ptkl, pts);  val dtYMD = TL.tryParse(fmtYMD, ptvYMD)      
        if( dtYMD.before(mindt) ) {
          HV.rmHDFSFolder(ssc, s"$tbpath/$pts")
          val hiveptexpr = HT.getPTEFromPTS(pts); val sqldrop = s"ALTER TABLE $hivetb DROP IF EXISTS PARTITION ($hiveptexpr)"
          LG.info(s"minYMD: $minYMD, ptvYMD: $ptvYMD sqldrop: $sqldrop");ssc.sql(sqldrop)         
        }else {
          LG.info(s"minYMD: $minYMD, ptvYMD: $ptvYMD keep $pts")
        }
      }
 
      //clean up fix 02
      LG.info( s"minYMDx: $minYMD, dates:" + fixdates.mkString(",") )
      dfprts.map{ row =>
        val pts=row.get(0).toString(); val ptvYMD=HT.getYMDFromPTS(ptkl, pts);  val dtYMD = TL.tryParse(fmtYMD, ptvYMD)    
        if( dtYMD.before(mindt)  && fixdates.indexOf(ptvYMD) >= 0 ) {
          val expr = s"${ptkl}=${ptvYMD}"; val indx = pts.indexOf(expr)
          LG.info(s"expr: $expr, pts: $pts ")
          if( indx >= 0) {
            val path=pts.substring(0, indx + expr.length )
            val hdfspath = s"$tbpath/$path"; LG.info(s"minYMDx: $minYMD, ptvYMD: $ptvYMD, hdfspath: $hdfspath ")
            HV.rmHDFSFolder(ssc, hdfspath)
          }       
        }
      }
    } 

  }// end cleanExpiredData
}


class ORETL(s: SSSC, x: SSXC) extends TETLS {
  val tb     = s.sourceTable.split("\\.")(1).toUpperCase()
  val hivedb = s.targetTable.split("\\.")(0).toLowerCase()
  val hivetb = s.targetTable.split("\\.")(1).toLowerCase()
  
  def getm(orclc: ORC): TETLM = {
    val rp = orclc.s.targetTablePartitionRetentionPolicy
    val mb = orclc.orclcc.tabMB; val nspltx = (mb >> orclc.orclcc.blkNN)/8*8; val sparkpd= orclc.r.sparkParallelDegree
    val mm = if (s.targetTablePartitionColumn.isEmpty) {
      if ( mb < 512 || orclc.orclcc.vwTabs.length > 1) {
        new MNS(orclc)
      } else {
         LG.info(s"nspltx: $nspltx, sparkpd: $sparkpd")
        if (nspltx <= sparkpd) new MNL(orclc) else new MNH(orclc)
      }
    } else {
      if( !HV.tbExists(orclc.r.sparkSSC, hivetb) || HV.tbNoData(orclc.r.sparkSSC, hivetb) ) {
        LG.info("ORCLETL table not exists or table emtpy"); new MPCore(orclc) //MPCoreNew MPCore
      }else {
        val ls = orclc.x.targetTableLoadingStrategory
        if( ls == "FULL_LOADING" && rp == 0L ) {
          LG.info("MPF FULL_LOADING");                   new MPDFull(orclc)
        }else if ( ls == "INCREMENTAL_LOADING_CNT"){
           LG.info("MPD_CNT INCREMENTAL_LOADING_CNT");   new MPDCnt(orclc)
        }else if ( ls == "INCREMENTAL_LOADING_COB"){
           LG.info("MPD_CNT INCREMENTAL_LOADING_COB");   new MPDCob(orclc)
        }else if ( ls == "INCREMENTAL_LOADING_DRV"){
           LG.info("MPD_CNT INCREMENTAL_LOADING_DRV");   new MPDDrv(orclc)
        }else if ( ls == "TRUNCATE_LOADING"){
           LG.info("MPD_DRP INCREMENTAL_LOADING_DRP");   new MPDDrp(orclc)
        }else{
          LG.info("MPF DEFAULT");                        new MPCore(orclc)
        }
      }
    }
    mm
  }
  
  def r() = {
    val sw = SW().start
    val orcl = new ORP(s, x).p()
    sw.awpLog("SPRINTER INIT time:")

    orcl.r.sparkSSC.sql(s"USE  $hivedb")
    val m = getm(orcl)
    m.r()
    
    val benchmark=50; val speed=orcl.orclcc.tabMB/sw.seconds(); val ratio= math.ceil(100.0 * ( speed-benchmark ) / benchmark)
    val sx = s"speed: $speed mb/s (benchmark: $benchmark, ration: ${ratio}%), table: $tb, oracle_cpu: ${orcl.orclcc.cpuNN}  ,block_size: ${orcl.orclcc.tabBK} ,table_size: ${orcl.orclcc.tabMB} MB ,parallel: ${orcl.r.sparkParallelDegree}, file_format: ${orcl.s.targetTableFileFormat}"
    sw.awpLog("SPRINTER ETL  time:", sx )
    
    CTRK.ended()
    orcl.r.sc.stop()
  }  
}