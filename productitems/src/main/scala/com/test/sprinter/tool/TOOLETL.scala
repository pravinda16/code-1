package com.test.sprinter.tool

//   Time: 2017-01-23 ~ 2017-05-19

import com.test.sprinter.core._
import com.test.sprinter.util._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.hive.HiveContext

abstract class MTL(s: SSSC, x: SSXC, ssc: HiveContext) extends TETLM {
  val db     = s.sourceTable.split("\\.")(0).toUpperCase()
  val tb     = s.sourceTable.split("\\.")(1).toUpperCase()
  val fltexp = x.sourceTableFilterExpr
  val hivedb = s.targetTable.split("\\.")(0).toLowerCase()
  val hivetb = s.targetTable.split("\\.")(1).toLowerCase()
  val dbpath = s.targetTableLocation.getOrElse( HV.getDBFolder(ssc, hivedb) ) 
  val tbpath = s"$dbpath/$hivetb"
}


class TOOLETL(s: SSSC, x: SSXC) extends TETLS {
  def getm(s: SSSC, x: SSXC, ssc:HiveContext): TETLM = {
    val tool = CUTL.getMandatoryEnv("SPRINTER_TOOL")
    val mmmm = tool match {
      case "GENERATE_DSMT" => new MGenerateDSMT(s,x,ssc)
      case _                    => throw new SSE(s"invalid SPRINTER_TOOL, expected GENERATE_DB_SCHEMA got ${tool}")
      
    }
    mmmm
  }
  
  def r() = {
    val hivedb = s.targetTable.split("\\.")(0).toLowerCase()
    val hivetb = s.targetTable.split("\\.")(1).toLowerCase()
  
    if( !CUTL.getOptionalEnv("SPRINTER_GET_DRIVER_MEMORY").isEmpty ) {
      println(s"ROWROW_DMM 16")
      System.exit(0)
    }
    
    val sw = SW().start
 
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SPRINTER_OPC_" + hivetb)
    sparkConf.set("spark.executor.memory",                         "16g")
    sparkConf.set("spark.executor.cores",                          "1")
    sparkConf.set("spark.executor.instances",                      "16")
    
    val sc  = new SparkContext(sparkConf); val ssc = new HiveContext(sc); CTRK.inited() 
    ssc.sql(s"use $hivedb")
    
    val m = getm(s,x, ssc)
    m.r()
    
    sw.awpLog("SPRINTER POC  time:" )
    sc.stop()
  }  
}