package com.test.sprinter.util

//   Time: 2017-01-24 ~ 2017-05-18

import com.test.sprinter.core._
import org.apache.spark.sql.{DataFrame, SQLContext}

object DS {
  def getDSOption(s: SSSC, sql: String): Map[String, String] = {
    val oo = Map(
        "driver"             -> "oracle.jdbc.OracleDriver",
        "url"                -> s.awpDatabaseURL.get,
        "fetchsize"          -> "1000",
        "user"               -> s.awpDatabaseUser.get,
        "password"           -> s.awpDatabasePassword.get,
        "dbtable"            -> sql)

    oo
  }

  //test run:change start
  def getLastUpdateDate(bc: SSBC): String = {
    val processId = bc.s.sprinterJobID.toString
    val sql =
      s"""(
         SELECT max(TO_CHAR(END_TIME,'YYYY-MM-DD')) AS UPDATE_DATE FROM OPSPORTAL.APP_EAP_PROCESS_DENM WHERE PROCESS_ID = '$processId'  AND OVERALL_STATUS = 'Completed'
) A """

    val awpCon = getDSOption(bc.s, sql);
    LG.logSparkOption(awpCon)
    val ssc = bc.r.sparkSSC
    val rows = ssc.read.format("jdbc").options(awpCon).load().collect()

    val lastUpdateDate = rows.map(rec => rec.getString(0))
    LG.info(s"lastUpdateDate:: ${lastUpdateDate.toList.mkString}")
    lastUpdateDate.mkString
  }
  //test run:change end

  def newColDDL(x: SSXC): Array[STCI] = {
    x.sourceTableNewColumnsName.zip(x.sourceTableNewColumnsType).map(r => STCI(r._1, r._2))
  }
  
  def getTCI(bc: SSBC): Array[STCI] = {
    LG.debug("getTCI")
    if (bc.s.sourceTableDataSetID.isEmpty) Array.empty[STCI] else getTCIu(bc)
  }

  def getTCI(dsTCI: Array[STCI], df: DataFrame, bc: SSBC): Array[STCI] = {
    val dcn = newColDDL(bc.x)
    if (dsTCI.size > 0) (dsTCI ++ dcn) else DF.getTCI(df, bc.r.sparkSSC)
  }

  def getTCI(dsTCI: Array[STCI], newTCI: Array[STCI], df: DataFrame, bc: SSBC): Array[STCI] = {
    if (dsTCI.size > 0) (dsTCI ++ newTCI) else DF.getTCI(df, bc.r.sparkSSC)
  }

  def reconHiveWithDS(ssc:SQLContext, hivetb: String, dsTCI:Array[STCI]): STCR = {
    val dsCols = dsTCI.map(r => r.n.toUpperCase)
    val dfTmp = ssc.sql(s"SELECT * FROM $hivetb LIMIT 1")
    val names = dfTmp.columns.map { n => n.toUpperCase() }
    val hive  = names.toSet;     val DS    = dsCols.toSet
    val difHV = hive.diff(DS) - "ORA_ROWSCN";   val difDS = DS.diff(hive)
    LG.debug("reconSchema present in HIVE, not in DSMT:" + difHV.toArray[String].mkString(","))
    LG.debug("reconSchema present in DSMT, not in HIVE:" + difDS.toArray[String].mkString(","))
    val sqlHV = difHV.map( r => s"ALTER TABLE $hivetb DROP COLUMN $r").mkString(";");
    val misDS = dsTCI.filter(r => difDS.contains(r.n.toUpperCase)).map(r => s"${r.n} ${r.t}").mkString(",")
    val sqlDS = if(misDS.isEmpty())"" else s"ALTER TABLE $hivetb ADD COLUMNS ($misDS)"; 
    val succ = if (difHV.isEmpty && difDS.isEmpty) true else false
    STCR(succ, sqlHV, sqlDS )
  }

  def reconSrcWithDS(ssc:SQLContext, df: DataFrame, dsTCI:Array[STCI]): STCR = {
    val dsCols = dsTCI.map(r => r.n.toUpperCase)
    val names = df.columns.map { n => n.toUpperCase() }
    val xxst  = Set("SPRINTER_ID", "ORA_ROWSCN", "SPRINTER_PTK", "SPRINTER_SNAPSHOT")
    val dfst  = names.toSet;     val dsst    = dsCols.toSet
    val difdf = dfst.diff(dsst) -- xxst;   val difds= dsst.diff(dfst) -- xxst
    val logdf = if(difdf.isEmpty)"" else "reconSchema present in SOURCE TABLE, not in DSMT:         " + difdf.toArray[String].mkString(",")
    val logds = if(difds.isEmpty)"" else "reconSchema present in DSMT,         not in SOURCE TABLE: " + difds.toArray[String].mkString(",")
    LG.debug(s"reconSrcWithDS result: $logdf and $logds")
    val succ = if (difdf.isEmpty && difds.isEmpty) true else false
    STCR(succ, logdf, logds )
  }
   
  def reconTableSchema(src:DataFrame, tgt:DataFrame): String = {
    val xx = src.schema.fields.map( x => ( x.name.toLowerCase(), x.dataType.simpleString ) ).toMap 
    val yy = tgt.schema.fields.map( x => ( x.name.toLowerCase(), x.dataType.simpleString ) ).toMap
    val ff = yy.filter(r => xx.contains(r._1)).map{ nt =>
      val n = nt._1; val t = nt._2; val st = xx.get(n).get
      val r = if (st != t) {
        s"recon_schema name: $n, type: db(${st}) vs hive/dsmt(${t})"
      } else {
        s"recon_schema name: $n, type: db(${st}) vs hive/dsmt(${t}) MATCHED"
      }
      r
    }
    ff.filter( !_.contains("MATCHED") ).mkString("\n")
  }
  
  def reconDSDate(bc: SSBC, db:String, tb:String):String = {
    val dtCols  = DS.getDTColsFromDS(bc)
    val tsCols  = DS.getTSColsFromDS(bc)
    val xcols   = (dtCols ++ tsCols).distinct
    
    if (xcols.length == 0) "" else {
      val allcols = xcols.map(c => s"$c as $c").mkString(",")
      val q = s"( SELECT TOP 1 $allcols  FROM  $db.$tb ) X "
      val ssc = bc.r.sparkSSC
      val jo = CUTL.getOption(bc.s, q, 1000); LG.logSparkOption(jo)
      val df = ssc.read.format("jdbc").options(jo).load()
      val rs = df.schema.fields.map(x => (x.name.toLowerCase(), x.dataType.simpleString)).map { nt =>
        val n = nt._1; val t = nt._2
        if (t == "timestamp") "MATCHED" else s"recon_schema name: $n, type: db($t) vs dsmt date/timestamp"
      }
      rs.filter(!_.contains("MATCHED")).mkString("\n")
    }
  }
  
  def tansformFix(t:String):String = {
    t.replaceAll("\\.", ",").replaceAll("_", ",")
  }
  
  def mapCTOracle(t: String, transform: String): String = {
    val transformx = if( transform==null || transform.isEmpty() ) "" else tansformFix(transform)
    val nt = t match {
      case "S"     => "STRING"
      case "N"     => if( transformx.isEmpty() )  "DECIMAL(38, 0)" else {
        val newtf = transformx.toUpperCase() match {
          case "FLOAT" => "FLOAT"
          case "INT"   => "INT"
          case _       => s"DECIMAL($transformx)"
        }
        newtf
      }
      case "FLOAT" => "DECIMAL(38,10)"    
      case _       => "ERROR"
    }    
    if (nt == "ERROR") {
      throw new SSE(s"not supported DS column type: $t")
    }
    nt
  }
 
  def mapCTTeradata(t: String, transform: String): String = {
    val transformx = if( transform==null || transform.isEmpty() ) "" else tansformFix(transform)
    val nt = t match {
      case "S"       => "STRING"       
      case "N"       => if( transformx.isEmpty() ) "INT"   else {
        val newtf = transformx.toUpperCase() match {
          case "FLOAT" => "FLOAT"
          case "INT"   => "INT"
          case _       => s"DECIMAL($transformx)"
        }
        newtf
      }
      case "DECIMAL" => "DECIMAL(18, 5)"
      case _         => "ERROR"
    }    
    if (nt == "ERROR") {
      throw new SSE(s"not supported DS column type: $t")
    }
    nt
  }

  def mapCTNetezza(t: String, transform: String): String = {
    val transformx = if( transform==null || transform.isEmpty() ) "" else tansformFix(transform)
    val nt = t match {
      case "S"       => "STRING"       
      case "N"       => if( transformx.isEmpty() ) "INT"   else {
        val newtf = transformx.toUpperCase() match {
          case "FLOAT" => "FLOAT"
          case "INT"   => "INT"
          case _       => s"DECIMAL($transformx)"
        }
        newtf
      }
      case "DECIMAL" => "DECIMAL(18, 5)"
      case _         => "ERROR"
    }    
    if (nt == "ERROR") {
      throw new SSE(s"not supported DS column type: $t")
    }
    nt
  }

  def mapCTMemSQL(t: String, transform: String): String = {
    val transformx = if( transform==null || transform.isEmpty() ) "" else tansformFix(transform)
    val nt = t match {
      case "S"       => "STRING"       
      case "N"       => if( transformx.isEmpty() ) "INT"   else {
        val newtf = transformx.toUpperCase() match {
          case "FLOAT"    => "FLOAT"
          case "INT"      => "INT"
          case "BIGINT"   => "BIGINT"
          case _          => s"DECIMAL($transformx)"
        }
        newtf
      }
      case "DECIMAL" => "DECIMAL(18, 5)"
      case _         => "ERROR"
    }    
    if (nt == "ERROR") {
      throw new SSE(s"not supported DS column type: $t")
    }
    nt
  }
  
  
  def mapCT(t: String, fmt:String, cmd: String): String = {
    val hivet = cmd match {
      case "SPRINTER_ORA2HIVE" => mapCTOracle(t,   fmt)
      case "SPRINTER_TD2HIVE"  => mapCTTeradata(t, fmt)
      case "SPRINTER_NZ2HIVE"  => mapCTNetezza(t,  fmt)
      case "SPRINTER_MM2HIVE"  => mapCTMemSQL(t,   fmt)
      case "SPRINTER_POC"      => mapCTOracle(t,   fmt)
      case _                   => throw new SSE(s"invalid command, expected SPRINTER_ORA2HIVE/SPRINTER_TD2HIVE/SPRINTER_NZ2HIVE/SPRINTER_MM2HIVE got ${cmd}")
    }
    hivet
  }
  
  def getDTColsFromDS(bc: SSBC): Array[String] = {
    val dset = bc.s.sourceTableDataSetID.get
    val sql = s"""(
SELECT trim(ATTRIBUTE_NAME) AS ATTRIBUTE_NAME
FROM OPSPORTAL.MD_EAP_DATASET_ATTRIBUTES 
WHERE DATASET_ID_ = '$dset' AND trim(DATA_TYPE)='S' AND trim(TRANSFORM_PROPERTIES) = 'YYYY-MM-DD'
) A """

    val ooo  = getDSOption( bc.s, sql ); LG.logSparkOption(ooo)
    val ssc  = bc.r.sparkSSC
    val rows = ssc.read.format("jdbc").options(ooo).load().collect()

    if (rows.length <= 0) {
      LG.warn(s"invalid sourceTableDataSetID: $dset")
    }

    val dt_cols = rows.map ( row => row.getString(0).toUpperCase )
    LG.info("dt_cols: " + dt_cols.mkString(","))
    dt_cols
  }
 
  def getTSColsFromDS(bc: SSBC): Array[String] = {
    val dset = bc.s.sourceTableDataSetID.get
    val sql = s"""(
SELECT trim(ATTRIBUTE_NAME) AS ATTRIBUTE_NAME
FROM OPSPORTAL.MD_EAP_DATASET_ATTRIBUTES 
WHERE DATASET_ID_ = '$dset' AND trim(DATA_TYPE)='S' AND trim(TRANSFORM_PROPERTIES) LIKE '%YYYY-MM-DD HH24:MI:SS%'
) A """

    val ooo  = getDSOption( bc.s, sql ); LG.logSparkOption(ooo)
    val ssc  = bc.r.sparkSSC
    val rows = ssc.read.format("jdbc").options(ooo).load().collect()

    if (rows.length <= 0) {
      LG.warn(s"invalid sourceTableDataSetID: $dset")
    }

    val ts_cols=rows.map ( row => row.getString(0).toUpperCase )
    LG.info("ts_cols: " + ts_cols.mkString(","))
    ts_cols
  }
  
  def getTrimColsFromDS(bc: SSBC): Array[String] = {
    val dset = bc.s.sourceTableDataSetID.get
    val trim = bc.x.sourceTableTrimStringColumns.getOrElse("NO")
    val sql = s"""(
SELECT trim(ATTRIBUTE_NAME) AS ATTRIBUTE_NAME
FROM OPSPORTAL.MD_EAP_DATASET_ATTRIBUTES 
WHERE DATASET_ID_ = '$dset' AND trim(DATA_TYPE)='S' AND ( trim(TRANSFORM_PROPERTIES) ='TRIM' OR '$trim' = 'YES' )
) A """

    val ooo  = getDSOption( bc.s, sql ); LG.logSparkOption(ooo)
    val ssc  = bc.r.sparkSSC
    val rows = ssc.read.format("jdbc").options(ooo).load().collect()

    if (rows.length <= 0) {
      LG.warn(s"invalid sourceTableDataSetID: $dset")
    }

    val ts_cols=rows.map ( row => row.getString(0).toUpperCase )
    LG.info("ts_cols: " + ts_cols.mkString(","))
    ts_cols
  }
  
  def getTCIu(bc: SSBC): Array[STCI] = {
    val dset = bc.s.sourceTableDataSetID.get
    val sql = s"""(
SELECT trim(ATTRIBUTE_NAME) AS ATTRIBUTE_NAME, trim(DATA_TYPE) as DATA_TYPE, trim(TRANSFORM_PROPERTIES) as TRANSFORM_PROPERTIES, ATTR_ORDER 
FROM OPSPORTAL.MD_EAP_DATASET_ATTRIBUTES 
WHERE DATASET_ID_ = '$dset' 
ORDER BY ATTR_ORDER
) A """

    val ooo  = getDSOption( bc.s, sql ); LG.logSparkOption(ooo)
    val ssc  = bc.r.sparkSSC
    val rows = ssc.read.format("jdbc").options(ooo).load().collect()

    if (rows.length <= 0) {
      LG.error(s"invalid sourceTableDataSetID: $dset")
    }
    val cmd=bc.s.sprinterCommand
    val tmpa = rows.map { row =>
      row.getString(0).toUpperCase + "  " + mapCT(row.getString(1).toUpperCase, row.getString(2), cmd)
    } 
    val tmps = tmpa.mkString(",\n"); LG.info(s"DS detail for $dset: $tmps")

    val cdsi = rows.map { row =>
      STCI(row.getString(0).toUpperCase, mapCT(row.getString(1).toUpperCase,  row.getString(2), cmd) )
    }
    cdsi
  }
  
}