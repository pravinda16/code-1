package com.test.sprinter.util

//   Time: 2017-01-18 ~ 2017-05-26

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object HV {
  
  def tbExists(ssc: SQLContext, hivetb: String): Boolean = {
    try {
      ssc.sql(s"describe $hivetb")
      true
    } catch {
      case th: Throwable => {
        if (th.getMessage.contains("Table not found")) false else true
      }
    }
  }

  def tbNoData(ssc: SQLContext, hivetb: String): Boolean = {
    val tbe = tbExists(ssc, hivetb)
    var n:Long   = 0
    if( !tbe ) {
      LG.info(s"$hivetb DOES NOT EXISTS")
    }else {
      n = ssc.sql(s"SELECT 1 FROM $hivetb LIMIT 1").count()
      LG.info(s"$hivetb LIMIT 1 return $n")
    }
    !(n == 1)
  }
  
  def tbNotEmpty(ssc: SQLContext, tb: String): Boolean = {
    if (!tbExists(ssc, tb)) false else {
      try {
        ssc.sql(s"SELECT 1 FROM $tb LIMIT 1").count > 0
      } catch {
        case th: Throwable => {
          LG.warn(th.toString())
          false
        }
      }
    }
  }
  
  def tbPartioned(ssc: SQLContext, hivetb:String): Boolean = {
     if (!tbExists(ssc, hivetb)) false else {
      val data = ssc.sql(s"show create table $hivetb").collect().map(r => r.getString(0)).mkString("")
      if (data.contains("PARTITIONED BY (")) true else false
     }
  }
  
  def getDBFolder(ssc: SQLContext, hivedb: String): String = {
    val data = ssc.sql(s"describe database $hivedb").collect().map(r => r.getString(0))
    data(0).split("\\s+")(1)
  }

  def rmHDFSFolder(ssc: SQLContext, hdfsFolder: String): Unit = {
    if (!hdfsFolder.isEmpty) {
      val hadoopConf = ssc.sparkContext.hadoopConfiguration
      var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      LG.info(s"**** remove folder: $hdfsFolder")
      hdfs.delete(new org.apache.hadoop.fs.Path(hdfsFolder), true)
    }
  }

  def mkHDFSFolder(ssc: SQLContext, hdfsFolder: String): Unit = {
    if (!hdfsFolder.isEmpty) {
      val hadoopConf = ssc.sparkContext.hadoopConfiguration
      var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      LG.info(s"**** create folder: $hdfsFolder")
      hdfs.mkdirs(new org.apache.hadoop.fs.Path(hdfsFolder))
    }
  }
  
  def ckHDFSFolder(ssc: SQLContext, hdfsFolder: String): Boolean = {
    if (hdfsFolder.isEmpty) false else {
      val hadoopConf = ssc.sparkContext.hadoopConfiguration
      var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      hdfs.exists(new org.apache.hadoop.fs.Path(hdfsFolder))
    }
  }
    
  def getTBLocation(ssc: SQLContext, hivetb:String): Option[String] = {
    var tbLocation:String=""
     if (tbExists(ssc, hivetb)) {
      val data = ssc.sql(s"show create table $hivetb").collect().map(r => r.getString(0))
      if (data(0).contains("EXTERNAL")) {
        val folder = data.filter(r => r.contains(s"/$hivetb") && r.contains("hdfs:")).mkString("").replace("'", "").replace(" ", "")
        if (folder.startsWith("hdfs://") && folder.endsWith(s"/$hivetb")) {
          tbLocation = folder
        }else{
          LG.warn( s"****  table name not match: location: $folder table: $hivetb")
        }
      }
    }
    if( tbLocation.isEmpty() ) Option(null) else Some(tbLocation)
  }
  
  def rmStageTable(ssc: SQLContext, hivetb: String): Unit = {
    val location=getTBLocation(ssc, hivetb)
    if( !location.isEmpty ) {
      LG.info( s"**** remove tb: $hivetb, location:${location.get} "); rmHDFSFolder(ssc, location.get)
      val dpsql=s"DROP TABLE IF EXISTS ${hivetb}"; LG.debug(dpsql); ssc.sql(dpsql);
    } else{
       LG.warn( s"***** Unable to get external table location, tb: $hivetb")    
    }
  }

  def rmETTable(ssc: SQLContext, hivetb: String): Unit = {
    rmStageTable(ssc, hivetb)
  }
  
  def bkETTable(ssc: SQLContext, hivetb: String): Option[String] = {
    var bkLocation: String=""
    val tbLocation=getTBLocation(ssc, hivetb)
    if( !tbLocation.isEmpty ) {
      val oldFolder=tbLocation.get
      val newFolder=s"${oldFolder}_bk47"
      val hadoopConf = ssc.sparkContext.hadoopConfiguration
      var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val dpsql=s"DROP TABLE IF EXISTS ${hivetb}_bk47"; LG.debug(dpsql); ssc.sql(dpsql);
      val rnsql=s"ALTER TABLE $hivetb RENAME TO ${hivetb}_bk47"; LG.debug(rnsql); ssc.sql(rnsql);
      LG.info(s"**** backup table: $hivetb location: $oldFolder to $newFolder ")
      hdfs.rename(new org.apache.hadoop.fs.Path(oldFolder), new org.apache.hadoop.fs.Path(newFolder))
      bkLocation=newFolder
    } else{
       LG.awpLog( s"***** Unable to get external table location, tb: $hivetb")    
    }
    if( bkLocation.isEmpty() ) Option(null) else Some(bkLocation)
  }

  def rmETTableBk(ssc: SQLContext, hdfsFolder: String, hivetb: String): Unit = {
    rmHDFSFolder(ssc, hdfsFolder)
    val dpsql=s"DROP TABLE IF EXISTS ${hivetb}_bk47"; LG.debug(dpsql); ssc.sql(dpsql);   
  }
    
  def rsETTable(ssc: SQLContext, hivetb: String, bkPath: String): Unit = {
    val newPath = bkPath.replace("_bk47", ""); LG.info(s"**** restore table tb: $hivetb, location: $bkPath to $newPath ")
    val hadoopConf = ssc.sparkContext.hadoopConfiguration
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val dpsql=s"DROP TABLE IF EXISTS ${hivetb}"; LG.debug(dpsql); ssc.sql(dpsql);
    val rnsql=s"ALTER TABLE ${hivetb}_bk47 RENAME TO $hivetb"; LG.debug(rnsql); ssc.sql(rnsql);
    LG.info(s"**** remove folder: $newPath")
    hdfs.delete(new org.apache.hadoop.fs.Path(newPath), true)
    hdfs.rename(new org.apache.hadoop.fs.Path(bkPath), new org.apache.hadoop.fs.Path(newPath))
  }
  
  def fixETPartitionName(ssc: SQLContext, tmpxtbPath:String, hivetbPath: String, ptpath:String): Unit = {
    val srcFolder = s"$tmpxtbPath/${ptpath}"
    val dstFolder = s"$hivetbPath/${ptpath}"
    
    if (ckHDFSFolder(ssc, srcFolder))  {
      if( ckHDFSFolder(ssc, dstFolder) ) {
        LG.debug(s"**** fixETPartitionName remove exists target: $dstFolder hivetbPath: $hivetbPath")
        rmHDFSFolder(ssc, dstFolder)
      }   
      val hadoopConf = ssc.sparkContext.hadoopConfiguration
      var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      
      LG.info(s"**** fixETPartitionName rename source: $srcFolder target: $dstFolder hivetbPath: $hivetbPath")
      hdfs.rename(new org.apache.hadoop.fs.Path(srcFolder), new org.apache.hadoop.fs.Path(dstFolder))
    } else {
      LG.warn(s"**** fixETPartitionName folder: $srcFolder does not exist")
    }
  }
  
  def crtETPartitionPath(ssc: SQLContext, hivetbPath: String, ptpath:String): Unit = {
    val ptFolder = s"$hivetbPath/${ptpath}"
    if( !ckHDFSFolder(ssc, ptFolder) ) {
      mkHDFSFolder(ssc, ptFolder)
    }
  }
  
  def getPartitions(ssc: SQLContext, hivetb: String): Array[String] = {
    val dt = ssc.sql(s"show partitions  $hivetb")
    val pts = dt.collect().map(r => r.getString(0).split("=")(1).substring(0, 10)).sorted
    LG.debug(pts.mkString(","))
    pts
  }

  def dropPartition(ssc: SQLContext, hivetb: String, pte: String): Unit = {
     val sql = s"ALTER TABLE $hivetb DROP IF EXISTS PARTITION ($pte)"
     LG.debug(sql); ssc.sql(sql)
  }
 
  def getHdfsFiles(ssc: SQLContext, hivetbPath: String, fileExt:String=".parquet"): String = {
    val hadoopConf = ssc.sparkContext.hadoopConfiguration
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if (!hdfs.exists(new org.apache.hadoop.fs.Path(hivetbPath))) "" else {
      val files = hdfs.listStatus(new org.apache.hadoop.fs.Path(hivetbPath))
      val ffffs = files.map(_.getPath().toString()).filter(_.endsWith(fileExt))
      ffffs.mkString(",")
    }
  }
  
  def getTBF(ssc: SQLContext, hivetb: String): String = {
    val data = ssc.sql(s"show create table $hivetb").collect().map(r => r.getString(0)).mkString(" ")
    if( data.contains("ql.io.parquet"))    "PARQUET" else {
      if( data.contains("ql.io.avro") )    "AVRO" else {
        if( data.contains("field.delim") ) "CSV" else "UNKNOW"
      }
    }
  }

  def getAFF(tbfmt: String): Boolean = {
    tbfmt match {
      case "PARQUET" => true
      case "AVRO"    => true
      case "CSV"     => true
      case _         => false
    }
  }
  
  def fixAVRO(dt: String, flfmt: String): String = {
    if( flfmt != "AVRO" ) dt else {
      if( dt == "timestamp") "bigint" else dt //bigint
    }
  }

  def enableDP(ssc: SQLContext, maxPartitions:Int=1000000, maxPartitionsPerNode:Int=10000): Unit = {
    ssc.setConf("hive.exec.dynamic.partition",              "true")
    ssc.setConf("hive.exec.dynamic.partition.mode",         "nonstrict")
    ssc.setConf("hive.exec.max.dynamic.partitions",         maxPartitions.toString)
    ssc.setConf("hive.exec.max.dynamic.partitions.pernode", maxPartitionsPerNode.toString)
    ssc.sql("SET mapred.max.split.size=256000000")
  }

  
  def getExpiredDate(ssc: SQLContext, hivetb: String, datepk: String, minDate:String): Array[String] = {
    val lines  = ssc.sql(s"SHOW PARTITIONS $hivetb").collect().map(r => r.getString(0))
    if( lines.length == 0) Array.empty[String] else {
      val fileds = lines(0).split("/").map(y => y.split("=")(0))
      val schema = StructType(fileds.map(n => StructField(n, StringType, true)))

      val rows = lines.map(x => x.split("/").map(y => y.split("=")(1)))
      val rdd = ssc.sparkContext.parallelize(rows, 4).map(x => Row(x: _*))

      val df = ssc.createDataFrame(rdd, schema)
      df.registerTempTable("tmp_ptks")
      val sql = s"select distinct $datepk from tmp_ptks where $datepk < '$minDate' "
      val rs = ssc.sql(sql).collect().map(r => r.getString(0))
      rs
    }
  }
  
 
 def cleanExpiredData(ssc: SQLContext, AsOfDate:String, hivetb: String, ptkl: String, tbpath:String, flBkF: Boolean, retenp: Long) = {
    if(!flBkF && retenp > 0L ) {
      val fmtYMD = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance(); cal.setTime(fmtYMD.parse(AsOfDate)); cal.add(Calendar.DATE, -1 * (retenp-1).intValue)
      val mindt=cal.getTime; val minYMD=fmtYMD.format(mindt)
      val dfprts = ssc.sql(s"SHOW PARTITIONS $hivetb") 
      dfprts.collect().map{ row =>
        val pts=row.get(0).toString(); val ptvYMD=HT.getYMDFromPTS(ptkl, pts);  val dtYMD = TL.tryParse(fmtYMD, ptvYMD)      
        if( dtYMD.before(mindt) ) {
          HV.rmHDFSFolder(ssc, s"$tbpath/$pts")
          val hiveptexpr = HT.getPTEFromPTS(pts); val sqldrop = s"ALTER TABLE $hivetb DROP IF EXISTS PARTITION ($hiveptexpr)"
          LG.info(s"minYMD: $minYMD, ptvYMD: $ptvYMD sqldrop: $sqldrop");ssc.sql(sqldrop)         
        }else {
          LG.info(s"minYMD: $minYMD, ptvYMD: $ptvYMD keep")
        }
      }
    }  
  }// end cleanExpiredData
}