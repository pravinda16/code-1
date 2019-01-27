package com.test.sprinter.tool

//   Time: 2017-01-23 ~ 2017-05-20

import com.test.sprinter.core._
import com.test.sprinter.util._
import org.apache.spark.sql.hive.HiveContext

class MGenerateDSMT(s: SSSC, x: SSXC, ssc: HiveContext) extends MTL(s, x, ssc) {
  val sparkpd = 1

  def gq(dbt:String): String = {
    
    val sql = dbt match {
      case "ORACLE"   => s"select       * from ${db}.${tb} WHERE ROWNUM <= 1"
      case "TERADATA" => s"select TOP 1 * from ${db}.${tb} "
      case "NETEZZA"  => s"select       * from ${db}.${tb} LIMIT 1"
      case _          => throw new SSE(s"invalid SPRINTER_TOOL_DB, expected ORACLE/TERADATA/NETEZZA/MEMSQL got ${dbt}")
    }
    
    sql
  }
  
  def gor( schema:Map[String,String] ): String = {
    val dsmt = schema.map { r =>
      val nn = r._1; val tt = r._2
      var tp = ""
      val xx = tt match  {
        case "int"          =>"N, INT"
        case "float"        =>"N, FLOAT"
        case "date"         =>"S, YYYY-MM-DD"
        case "timestamp"    =>"S, YYYY-MM-DD HH24:MI:SS.FF5"
        case x if x.contains("decimal") => {
          if( tt == "decimal(38,0)" ) "N" else {
            tp=tt.replace("decimal(", "").replace(")", "").replace(",", ".")
            s"N, $tp"
          }
        }
         case  _             => throw new SSE(s"unknow data type $tt ")
      }
      xx
    }
    dsmt.mkString("\n")
  }
  
  def gnz( schema:Map[String,String] ): String = {
    "ok"
  }
  
  def gtd( schema:Map[String,String] ): String = {
    "ok"
  }
  
  def gn(dbt:String, schema:Map[String,String]):String = {
    val dsmt = dbt match {
      case "ORACLE"   => gor(schema)
      case "TERADATA" => gor(schema)
      case "NETEZZA"  => gor(schema)
      case _          => throw new SSE(s"invalid SPRINTER_TOOL_DB, expected ORACLE/TERADATA/NETEZZA/MEMSQL got ${dbt}")
    }
    dsmt
  }

  def r(): Unit = {
    LG.info("In MGenerateDBSchema")
    val dbt = CUTL.getMandatoryEnv("SPRINTER_TOOL_DB")
      
    val xo = CUTL.getXOption(sparkpd, 1, sparkpd + 1)
    val to = CUTL.getOption(s, gq(dbt), 100)
    val jo = to ++ xo; LG.logSparkOption(jo)

    val df = ssc.read.format("jdbc").options(jo).load().cache()
    val ts = df.schema.fields.map( x => ( x.name.toLowerCase(), x.dataType.simpleString ) ).toMap 
    val dsmt=gn(dbt, ts)
    LG.awpLog(dsmt)
  }

}