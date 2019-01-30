package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-03-01 ~ 2017-05-17

import com.citi.sprinter.core._
import com.citi.sprinter.util.HV.tbExists
import com.citi.sprinter.util._
import org.apache.spark.sql.Row

class MPDCnt(orcl: ORC) extends MORCL(orcl) {
  val allPtkl = orcl.s.targetTablePartitionColumn.get.toLowerCase
  val allPtku = orcl.s.targetTablePartitionColumn.get.toUpperCase
  val aptkl = allPtkl.split(",").map(r => r.trim())
  val aptku = allPtku.split(",").map(r => r.trim())

  //2017-07-02
  val _dtIdx = orcl.s.targetTablePartitionDateColumnIndex
  val dtIdx = if (_dtIdx < 0 || _dtIdx > aptkl.length) aptkl.length - 1 else _dtIdx - 1
  val tptkl = aptkl(dtIdx)
  val tptku = aptku(dtIdx)

  //pd94468:change start
  def getPartitionDatesSql(): String = {
    LG.info("In MPDCnt: getPartitionDates()...")
    val updateDate = DS.getLastUpdateDate(orcl)
    var sqlora: String = ""
    if (updateDate != null && updateDate != "") {

      val where = if (fltexp.isEmpty) "" else s" WHERE ${fltexp.get}"
      sqlora = if (where.isEmpty) {
        s"""(SELECT  distinct  TO_CHAR(PARTITION_DATE, 'YYYY-MM-DD') AS PARTITION_DATES FROM $db.$tb WHERE TO_CHAR(LAST_UPDATED_DATE, 'YYYY-MM-DD') >= '$updateDate' ORDER BY PARTITION_DATES ) y"""
      }
      else {
        s"""(SELECT  distinct  TO_CHAR(PARTITION_DATE, 'YYYY-MM-DD') AS PARTITION_DATES FROM $db.$tb $where AND TO_CHAR(LAST_UPDATED_DATE, 'YYYY-MM-DD') >= '$updateDate' ORDER BY PARTITION_DATES ) y"""
      }
    }
    LG.info(s"SQL for getPartitionDates()::  $sqlora")
    sqlora.toString
  }

  def gelAllColumnsSql(): String = {
    val sql =
      s"""(SELECT COLUMN_NAME  as COL_NAME FROM  DBA_TAB_COLUMNS WHERE OWNER = '$db' AND TABLE_NAME  = '$tb' ORDER BY COLUMN_ID ) y"""
    LG.info(s"SQL for gelAllColumnsSql:: $sql")
    sql.toString
  }

  def isPartitionTableSql(): String = {
    val sql =
      s"""(SELECT PARTITIONED  as PARTITIONED FROM  DBA_TAB_COLUMNS WHERE OWNER = '$db' AND TABLE_NAME  = '$tb' ) y"""
    LG.info(s"SQL for isPartitionTableSql:: $sql")
    sql.toString
  }

  //pd94468:change end

  def r(): Unit = {
    LG.info("In MPDCnt");
    val sw = SW().start

    val where = if (fltexp.isEmpty) "" else s" WHERE ${fltexp.get}"
    val colsx = if (aptkl.length <= 1) "" else aptku.filter(r => r != tptku).mkString(",") + ","
    val sqlora = s"""( SELECT $colsx TO_CHAR($tptku, 'YYYY-MM-DD') AS $tptku,  TO_CHAR( COUNT(*) ) AS CNT FROM $db.$tb $where GROUP BY $colsx TO_CHAR($tptku, 'YYYY-MM-DD') ) y"""
    val optora = CUTL.getOption(orcl.s, sqlora, 1000);
    LG.debug(sqlora)
    val dfora = ssc.read.format("jdbc").options(optora).load().cache()
    dfora.registerTempTable("oratab123");
    val mindt = ssc.sql(s"SELECT MIN($tptku) FROM oratab123").first().getString(0)
    LG.info(s"mindt: $mindt")

    //pd94468:change start
    if (!orcl.s.sourceDatabaseID.isEmpty) LG.info(s"sourceDatabaseID : ${orcl.s.sourceDatabaseID.get}")
    val isHistoricalUpdateDB = orcl.s.sourceDatabaseID.get.trim.equalsIgnoreCase("TW") || orcl.s.sourceDatabaseID.get.trim.equalsIgnoreCase("FRESH")
    val oragelAllColumns = CUTL.getOption(orcl.s, gelAllColumnsSql(), jdbcfs)
    val colDF = ssc.read.format("jdbc").options(oragelAllColumns).load()
    colDF.registerTempTable("oracoltab123")
    LG.info(s"colDF Schema: ${colDF.printSchema()}")
    val count = ssc.sql(s"SELECT COUNT(1) FROM oracoltab123 WHERE COL_NAME = 'LAST_UPDATED_DATE' ").first().getLong(0)

    if (isHistoricalUpdateDB && count == 1) {
      val oraPartitionDates = CUTL.getOption(orcl.s, getPartitionDatesSql(), jdbcfs)
      val partitionDatesDF = ssc.read.format("jdbc").options(oraPartitionDates).load().cache()
      partitionDatesDF.registerTempTable("partitionstab123");
      val partition_dates = ssc.sql(s"SELECT distinct PARTITION_DATES FROM partitionstab123").collect()
      LG.info(s"Total number of partitions for updates: ${partition_dates.length}")
      LG.info(s"List of Partitions to drop: ${partition_dates.mkString(",")}")
      //code to drop hive partitions to handle updates
      ssc.sql(s"use ${hivedb}")
      if (tbExists(ssc, hivetb)) {
        val genpexp = (r: Row) => {
          val totalPartitions = for (i <- 0 to r.length - 1) yield r.getString(i)
          LG.info(s"Partition for Update:  ${totalPartitions.foreach(print)}")
          aptkl.zip(totalPartitions).map(x => s"${x._1}='${x._2}'").mkString(", ")
        }
        LG.info(s"Generating the expression for updating partition...")

        partition_dates.map(genpexp(_)).sorted.map(HV.dropPartition(ssc, hivetb, _))

      }
      partitionDatesDF.unpersist()
    }
    //pd94468:change end

    val sqlhiv = s"SELECT  $allPtku, CAST( COUNT(*) AS STRING) AS CNT FROM $hivetb WHERE $tptku >= '$mindt' GROUP BY $allPtku"
    val dfhiv = ssc.sql(sqlhiv).cache();
    LG.debug(sqlhiv)
    val dfnew = dfora.except(dfhiv);
    dfnew.cache();
    dfnew.printSchema();
    dfnew.show(10000, false)
    val dstct = dfnew.count();
    sw.debug(s"SPRINTER CNT  time: dstct: $dstct")
    val rows = dfnew.collect();
    dfhiv.unpersist();
    dfora.unpersist();
    dfnew.unpersist()

    if (dstct <= 0) {
      LG.awpLog("NO NEW DATA")
      cleanExpiredData(tptkl)
    } else {
      val fExpr = rows.map { row =>
        val rval = (0 to (aptku.length - 2)).map { i =>
          val tmpv = row.getString(i)
          val tmpn = aptku(i)
          s"$tmpn = '$tmpv' "
        }
        val dtv = row.getString(aptku.length - 1)
        val dte1 = rval.mkString(" AND ")
        val dte2 = s"$tptku = TO_DATE('$dtv', 'YYYY-MM-DD')"
        if (dte1.isEmpty()) dte2 else s"( $dte1  AND  $dte2 )"
      }.mkString(" OR \n")
      val nw = if (dstct < 186) s"( $fExpr )" else "";
      LG.info(s"LAST UPDATE TIME, FILTER EXPRESS: $nw")
      val nx = CUTL.dumpSSXC(orcl.x, nw)
      val norcl = ORC(orcl.s, nx, orcl.r, dumpOrcl(true, true))
      val m = new MPCore(norcl)
      m.r()
    }
  }
}