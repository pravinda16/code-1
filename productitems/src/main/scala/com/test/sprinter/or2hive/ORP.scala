package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-01-23 ~ 2017-05-16

import java.sql.{Connection, DriverManager}

import com.citi.sprinter.core._
import com.citi.sprinter.util._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

class ORP(s: SSSC, x: SSXC) extends TETLP {
  val db = s.sourceTable.split("\\.")(0).toUpperCase()
  val tb = s.sourceTable.split("\\.")(1).toUpperCase()

  def createDC(): Connection = {
    val driver = "oracle.jdbc.OracleDriver"
    var connection: Connection = null
    var times = 0
    var flag = 0

    while (times <= 3 && flag == 0) {
      try {
        Class.forName(driver)
        times = times + 1
        connection = DriverManager.getConnection(s.sourceDatabaseURL, s.sourceDatabaseUser, s.sourceDatabasePassword.get)
        flag = flag + 1
      } catch {
        case e: Exception => e.printStackTrace
      }
    }

    if (flag > 0) connection else throw new SSE("Failed to connect to source data")
  }

  def checkTE(conn: Connection): Boolean = {
    var cnt: Long = 0
    val sql = s"SELECT COUNT(1) as CNT FROM DBA_TABLES WHERE OWNER='$db' AND TABLE_NAME='$tb' ";
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cnt = rs.getBigDecimal("CNT").longValue()
      }
      rs.close()
      if (cnt == 1) true else false
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

  def checkVE(conn: Connection): Boolean = {
    var cnt: Long = 0
    val sql = s"SELECT COUNT(1) as CNT FROM DBA_VIEWS WHERE OWNER='$db' AND VIEW_NAME='$tb' ";
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cnt = rs.getBigDecimal("CNT").longValue()
      }
      rs.close()
      if (cnt == 1) true else false
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

  def getTabs(conn: Connection): Array[String] = {
    var cols = scala.collection.mutable.ArrayBuffer.empty[String]
    val sql =
      s"""
SELECT DISTINCT REFERENCED_OWNER, REFERENCED_NAME
FROM (
SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
FROM       DBA_DEPENDENCIES
START WITH OWNER = '$db' AND NAME = '$tb'
CONNECT BY  OWNER  = PRIOR REFERENCED_OWNER  AND  
             NAME  = PRIOR REFERENCED_NAME   AND TYPE <> 'TABLE'
) WHERE REFERENCED_TYPE='TABLE' """
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cols += (rs.getString("REFERENCED_OWNER") + ";" + rs.getString("REFERENCED_NAME"))
      }
      rs.close()
      cols.toArray
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

  def getTS(conn: Connection, cdb: String, ctb: String): (Long, Long) = {
    var mb: Long = 0
    var bk: Long = 0
    val sql =
      s"""
SELECT  NVL(SUM(BLOCKS),0) AS BK, NVL(ROUND(CEIL(SUM(BYTES)/1024/1024),0),0) AS MB
FROM DBA_SEGMENTS 
WHERE OWNER      =  '$cdb' 
AND SEGMENT_NAME =  '$ctb'
AND SEGMENT_TYPE IN( 'TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION') """
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        bk = rs.getBigDecimal("BK").longValue()
        mb = rs.getBigDecimal("MB").longValue()
      }
      rs.close()
      (bk, mb)
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

  def getTCols(conn: Connection): Array[String] = {
    var cols = scala.collection.mutable.ArrayBuffer.empty[String]
    val sql =
      s"""
SELECT COLUMN_NAME 
FROM  DBA_TAB_COLUMNS
WHERE OWNER = '$db' AND TABLE_NAME  = '$tb'
ORDER BY COLUMN_ID  """
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cols += rs.getString("COLUMN_NAME").trim()
      }
      rs.close()
      cols.toArray
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw new SSE(s"unable to get table cols, ${db}.${tb}, error: ${e.toString()}")
      }
    }
  }

  //pd94468: change start
  def checkIfColExists(conn: Connection): Boolean = {
    var cnt: Int = 0
    val sql =
      s"""
SELECT COUNT(1) as CNT
FROM  DBA_TAB_COLUMNS
WHERE OWNER = '$db' AND TABLE_NAME  = '$tb' AND COLUMN_NAME = 'LAST_UPDATED_DATE'  """
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cnt = rs.getInt("CNT").intValue()
      }
      rs.close()
      if (cnt == 1) true else false
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw new SSE(s"Unable to check if column 'LAST_UPDATED_DATE' exists in: ${db}.${tb}, error: ${e.toString()}")
      }
    }
  }

  //pd94468:change end

  def getTDateCols(conn: Connection): Array[String] = {
    var cols = scala.collection.mutable.ArrayBuffer.empty[String]
    val sql =
      s"""
SELECT COLUMN_NAME 
FROM  DBA_TAB_COLUMNS
WHERE OWNER = '$db' AND TABLE_NAME  = '$tb' AND DATA_TYPE IN ( 'DATE') 
ORDER BY COLUMN_ID """
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cols += rs.getString("COLUMN_NAME").trim()
      }
      rs.close()
      cols.toArray
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw new SSE(s"unable to get table size, ${db}.${tb}, error: ${e.toString()}")
      }
    }
  }

  def getTTimestampCols(conn: Connection): Array[String] = {
    var cols = scala.collection.mutable.ArrayBuffer.empty[String]
    val sql =
      s"""
SELECT COLUMN_NAME 
FROM  DBA_TAB_COLUMNS
WHERE OWNER = '$db' AND TABLE_NAME  = '$tb' AND DATA_TYPE LIKE '%TIMESTAMP%'
ORDER BY COLUMN_ID  """
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cols += rs.getString("COLUMN_NAME").trim()
      }
      rs.close()
      cols.toArray
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw new SSE(s"unable to get table size, ${db}.${tb}, error: ${e.toString()}")
      }
    }
  }


  def getCPU(conn: Connection): Int = {
    var cpu: Int = 0
    val sql = "SELECT ceil(value/8)*8 as CPU FROM  v$parameter WHERE name='cpu_count'"
    LG.debug(sql)

    try {
      val statement = conn.createStatement
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        cpu = rs.getBigDecimal("CPU").intValue()
      }
      rs.close()
      cpu
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

  def getORCLCC(): ORCC = {
    val sw = SW().start
    val conn = createDC()
    sw.debug("ORCLCC createDC")

    val tbe = checkTE(conn)
    if (!tbe && !checkVE(conn)) {
      conn.close()
      throw new SSE(s"table/view does not exist: $tb ")
    }
    sw.debug("ORCLCC checkTE")

    val vwTabs = if (tbe) Array.empty[String] else getTabs(conn)
    val ts = if (tbe) getTS(conn, db, tb) else {
      var maxv = (0l, 0l)
      vwTabs.map { r =>
        val towner = r.split(";")(0)
        val ttname = r.split(";")(1)
        val tttmpv = getTS(conn, db, tb)
        if (tttmpv._1 > maxv._1) {
          maxv = tttmpv
        }
      }
      maxv
    }
    sw.debug("ORCLCC getTS")

    val tbcols = getTCols(conn)
    val dtCols = getTDateCols(conn)
    val tsCols = getTTimestampCols(conn)
    val blkNN = CUTL.getOptionalEnvx("SPRINTER_X_BLOCK").getOrElse("10").toInt

    val cpu = getCPU(conn) * 2
    ORCC(ts._1, ts._2, cpu, blkNN, vwTabs, tbcols, dtCols, tsCols, 0, false)
  }

  def p(): ORC = {
    val orcl = getORCLCC()
    LG.debug(orcl.toString())

    val mb = orcl.tabMB.toInt
    val amb = Array[Int](64, 1024, 8 * 1024, 1024 * 16, 1024 * 64, 1024 * 512, 1024 * 1024, 8 * 1024 * 1024, 32 * 1024 * 1024, Int.MaxValue)
    val spd = Array[Int](4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048)
    val ccc = Array[Int](1, 2, 4, 4, 2, 2, 1, 1, 1, 1)
    val ddd = Array[Int](2, 2, 4, 8, 16, 16, 24, 24, 24, 24)
    val eee = Array[Int](2, 4, 8, 16, 16, 16, 16, 16, 16, 16)

    val sparkDriverMemory = TL.getOrFindTable("SPRINTER_X_DRIVER_MEMORY", amb, ddd, mb)
    if (!CUTL.getOptionalEnv("SPRINTER_GET_DRIVER_MEMORY").isEmpty) {
      println(s"ROWROW_DMM $sparkDriverMemory")
      System.exit(0)
    }

    val _sparkParallelDegree = CUTL.getOptionalEnvx("sparkParallelDegree").getOrElse("0").toInt

    val sparkMaxParallelDegree = CUTL.getOptionalEnvx("SPRINTER_X_MAX_PARALLEL").getOrElse("64").toInt
    val sparkExecutorCores = TL.getOrFindTable("SPRINTER_X_EXECUTOR_CORES", amb, ccc, mb)
    val sparkExecutorMemory = TL.getOrFindTable("SPRINTER_X_EXECUTOR_MEMORY", amb, eee, mb)
    val sourceTableJDBCFetchSize = CUTL.getOptionalEnvx("SPRINTER_X_JDBC_FETCH_SIZE").getOrElse("10000").toInt
    val spark_colsotre_batch_size = CUTL.getOptionalEnvx("SPRINTER_X_COLUMNARSTORAGE_BATCHSIZE").getOrElse("100000").toLong

    val sparkConf = new SparkConf()
    sparkConf.setAppName("SPRINTER_" + s.sprinterJobID + "_" + s.sprinterRunID)
    sparkConf.set("spark.driver.memory", s"${sparkDriverMemory}g")
    sparkConf.set("spark.executor.memory", s"${sparkExecutorMemory}g")
    sparkConf.set("spark.executor.cores", sparkExecutorCores.toString)
    sparkConf.set("spark.sql.inMemoryColumnarStorage.batchSize", spark_colsotre_batch_size.toString)
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.set("spark.shuffle.memoryFraction", "0.5")
    sparkConf.set("spark.yarn.executor.memoryOverhead", "4096")

    val instances = CUTL.getOptionalEnvx("SPRINTER_X_EXECUTOR_INSTANCES").getOrElse("").toString
    if (!instances.isEmpty) {
      sparkConf.set("spark.executor.instances", instances)
      LG.info(s"Number of sparkExecutorInstances: ${instances}")
    }

    val sc = new SparkContext(sparkConf)
    val sparkSSC = new HiveContext(sc)
    sparkSSC.setConf("spark.sql.parquet.compression.codec", x.targetTablecompressCodec)
    if (!s.targetTablePartitionColumn.isEmpty) HV.enableDP(sparkSSC)
    CTRK.inited()

    //average size
    val ideaParallelDegree = TL.findTab(amb, spd, mb)
    val sparkParallelDegree = if (_sparkParallelDegree > 0) _sparkParallelDegree else math.min(math.min(orcl.cpuNN, ideaParallelDegree), sparkMaxParallelDegree)

    LG.info(s"sparkExecutorCores: $sparkExecutorCores ; sparkExecutorMemory: ${sparkExecutorMemory}g table: $mb MB") //pd94468 change
    LG.info(s"ideaParallelDegree: $ideaParallelDegree ; JDBC sparkParallelDegree: $sparkParallelDegree")

    val r = SSRC(
      sc,
      sparkSSC,
      sparkMaxParallelDegree,
      sparkDriverMemory,
      sparkExecutorCores,
      sparkExecutorMemory,
      sourceTableJDBCFetchSize,
      ideaParallelDegree,
      sparkParallelDegree,
      0,
      0)

    new ORC(s, x, r, orcl)
  }
}
