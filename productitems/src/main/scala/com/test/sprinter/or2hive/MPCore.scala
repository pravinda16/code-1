package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-01-26 ~ 2017-05-27

import com.citi.sprinter.core._
import com.citi.sprinter.util._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.col

class MPCore(orcl: ORC) extends MORCL(orcl) {
  val hvstg = s"${hivetb}_stage"
  val hvtmp = s"${hivetb}_y_fx_z"
  val pathtmp = s"$dbpath/$hvtmp"
  val pathstg = s"$dbpath/$hvstg"
  val dftabnm = s"${hivetb}_df"
  val allPtkl = orcl.s.targetTablePartitionColumn.get.toLowerCase
  val allPtku = orcl.s.targetTablePartitionColumn.get.toUpperCase
  val aptkl   = allPtkl.split(",").map( r => r.trim() ) 
  val aptku   = allPtku.split(",").map( r => r.trim() ) 

  //2017-07-02
  val _dtIdx  = orcl.s.targetTablePartitionDateColumnIndex
  val dtIdx   = if( _dtIdx < 0 || _dtIdx > aptkl.length ) aptkl.length - 1 else _dtIdx - 1
  val tptkl   = aptkl( dtIdx )
  val tptku   = aptku( dtIdx )
 
  def gd(dci: Array[STCI]): String = {
    val fa = (aptku :+ "SPRINTER_ID")
    val cs = dci.filter( TL.fTbCols(_)(fa) ).map{ r => 
      "%-30s %s".format( r.n, TL.fixAVRO(r.t, flfmt) )
    }.mkString(", \n")
    
    val ps = aptkl.map( p => dci.filter( r => r.n.toLowerCase == p).head ).map{ r =>
      "%-30s %s".format( r.n, TL.fixAVRO(r.t, flfmt) )
    }.mkString(", \n")
    
    val tblfmt = TL.getTFD(flfmt)
    s"CREATE EXTERNAL TABLE $hivetb (\n$cs\n) PARTITIONED BY ($ps) $tblfmt LOCATION '$tbpath' " 
  }
  
  def gcnt(): Long = {
    val minYMD=getMinYMD(); 
    val sql =  if( retenp < 1) s"select count(*) from $dftabnm" else s"select count(*) from $dftabnm where $tptkl >= '$minYMD' "
    LG.info(sql);ssc.sql(sql).first().getLong(0)  
  }
  
  def gdst():String = {
    s"SELECT DISTINCT $allPtkl FROM $dftabnm "
  }
  
  def r(): Unit = {
    LG.info("In MPCore")
    if (!flBkF && dsTCI.length>0 && HV.tbNotEmpty(ssc, hivetb)) {
      val recon=DS.reconHiveWithDS(ssc, hivetb, dsTCI)
      if( !recon.succ ) {
        if( !recon.inDSMT.isEmpty())        LG.error (s"ERROR: Columns present in DSMT, but not in Hive table, SQL to fix: ${recon.inDSMT}")
        if( !recon.inHiveOrSrc.isEmpty())   LG.awpLog(s"WARN:  Columns present in Hive, but not in DSMT setup, SQL to fix: ${recon.inHiveOrSrc}")
      }
    }
    
    val norcl = ORC(CUTL.dumpSSSC(orcl.s, s"${hivedb}.${hvstg}") , orcl.x, orcl.r, dumpOrcl(true, false))
    val m = if ( orcl.orclcc.vwTabs.length > 1) new MNS(norcl) else {
      if( nspltx <= sparkpd ) new MNL(norcl) else new MNH(norcl)
    }
    m.r()

    val df=ssc.read.parquet(pathstg); df.registerTempTable(dftabnm)
    val tci = DS.getTCI(dsTCI, df, orcl)
    val tcx = DF.ogTCIx(tci,  aptkl);  LG.debug( tcx.map(r => r.n.toLowerCase).mkString(",") )
    val pkt = DF.getPKT(tcx,  tptkl);  LG.info(s"partition column: allPtkl, last ptk: $tptkl types: $pkt" )

    if( pkt.length <= 0 ) {
      val dsID = orcl.s.sourceTableDataSetID.getOrElse("")
      LG.error(s"Partition column does not exist: $allPtkl, please check table schema: $tb and  datasetID: $dsID")
    }
    
    if (!HV.tbExists(ssc, hivetb)) {
      val ddl = gd(tcx); LG.debug(ddl); ssc.sql(ddl)
    }
    
    val sw = SW().start
    val cnt=gcnt(); val sqld = gdst(); LG.info(sqld)
    val dstnctdf = ssc.sql(sqld).cache; dstnctdf.show(10, false)
    val rows = dstnctdf.collect(); val tz = rows.length

    val crtpath = (r: Row) => {
      val ptkv = for (i <- 0 to r.length-2) yield r.getString(i)
      aptkl.zip( ptkv ).map( x => s"${x._1}=${x._2}" ).mkString("/")
    }  
    val genpath = (r: Row) => {
      val ptkv = for (i <- 0 to r.length-1) yield r.getString(i)
      aptkl.zip( ptkv ).map( x => s"${x._1}=${x._2}" ).mkString("/")
    }   
    val genpexp = (r: Row) => {
      val ptkv = for (i <- 0 to r.length-1) yield r.getString(i)
      aptkl.zip( ptkv ).map( x => s"${x._1}='${x._2}'" ).mkString(",")
    }
    
    val oss = orcl.x.sourceTableNewColumnsName.filter( r => r.toUpperCase() == tptku ).length == 1 && aptku.length == 1
    LG.info(s"""part_num: $ideapd, part_cols: ${aptkl.mkString(",")}, path: $pathtmp distinct ptk: $tz, net count: $cnt, flag: $oss""" )
    
    val cols = aptkl.map( r => col(r) )
    if (oss) {
      df.drop(tptku); rows.map(genpath(_)).map { p => val osspath = s"${pathtmp}/${p}"; LG.debug(s"osspath: $osspath"); df.write.mode(SaveMode.Overwrite).parquet(osspath) }
    } else {
      df.repartition(ideapd, cols: _*).write.partitionBy(aptkl: _*).option("basepath", pathtmp).mode(SaveMode.Overwrite).parquet(pathtmp)
    }
    
    rows.map(crtpath(_)).sorted.map(HV.crtETPartitionPath(ssc, tbpath, _))
    rows.map(genpath(_)).sorted.map(HV.fixETPartitionName(ssc, pathtmp, tbpath, _))
    rows.map(genpexp(_)).sorted.map(HV.dropPartition(ssc, hivetb, _))
 
    val fids=rows.map(genpath(_)).sorted.map( r => HV.getHdfsFiles(ssc, s"${tbpath}/${r}"))
    LG.awpFileIds( fids.mkString(",")) 
      
    ssc.sql(s"MSCK REPAIR TABLE $hivetb")
    sw.awpLog("SPRINTER ORG  time:", s"fix partition name, total partitions: $tz, method: partitionBy")
    
    cleanExpiredData(tptkl)    
    HV.rmStageTable(ssc, hvstg); ssc.sql(s"DROP TABLE IF EXISTS $hvstg")
    HV.rmHDFSFolder(ssc, pathtmp)
    sw.awpLog("SPRINTER CPT  time:", s"targetTablePartitionRetentionPolicy value: $retenp, total partitions: $tz")
    
    if( tz == 0 && !fltexp.isEmpty && nodataE) {
      LG.error(s"No data avaible for sourceTableDisableNoDataException: ${nodataE} fltexp: ${fltexp.get}") 
    }
    LG.awpStat(s"$cnt")
  }  
}