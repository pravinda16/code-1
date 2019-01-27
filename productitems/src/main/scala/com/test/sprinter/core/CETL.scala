package com.test.sprinter.core

//   Time: 2017-01-19

import java.io.IOException

import com.test.sprinter.util._
import org.apache.spark.SparkException

object CETL {
    
  def main(args: Array[String]): Unit = {
    val sw = SW().start
    println(CVER.getBanner())
    var code=0
    var rflag=false
    var retry=0
    var errmsg:String=null
    
    do {
      try {
        val argMap = CUTL.argumentToMap(args)
        val ss = CUTL.getSprinterConext(argMap)
        CUTL.VerifySScontxt(ss)
        CUTL.getSparkPWD(ss)
        
        val xx = CUTL.getSprinterExConext(argMap)
        CUTL.verifySXcontxt(ss, xx)
        
        val sprinter = ss.sprinterCommand match {
          case "SPRINTER_ORA2HIVE" => new com.test.sprinter.or2hive.ORETL(ss, xx)
          case "SPRINTER_TD2HIVE"  => new com.test.sprinter.td2hive.TDETL(ss, xx)
          case "SPRINTER_NZ2HIVE"  => new com.test.sprinter.nz2hive.NZETL(ss, xx)
          case "SPRINTER_MM2HIVE"  => new com.test.sprinter.mm2hive.MMETL(ss, xx)
          case "SPRINTER_POC"      => new com.test.sprinter.tool.TOOLETL(ss, xx)
          case _                   => throw new SSE(s"invalid command, expected SPRINTER_ORA2HIVE/SPRINTER_TD2HIVE/SPRINTER_NZ2HIVE/SPRINTER_MM2HIVE got ${ss.sprinterCommand}")
        }
        sprinter.r()
        rflag=false
        
      } catch {
        case se: SSE => {
          se.printStackTrace()
          LG.awpLog(se.emsg)
          errmsg = se.emsg
          code = 1
          rflag=false
        }
        case ioe: IOException => {
          errmsg = ioe.getMessage
          ioe.printStackTrace()
          retry = retry + 1
          rflag = if( retry <= 3 && CTRK.notInited ) true else false
          if (!rflag) {
            LG.awpLog(ioe.toString())
            code = 2
          }
        }
        case sparke: SparkException => {
          errmsg = sparke.getMessage
          if( CTRK.endFlag) {
            LG.warn("Supress SPARK bug https://issues.apache.org/jira/browse/SPARK-14228" + sparke.getMessage )
            code = 0
            rflag=false
          }else {
            sparke.printStackTrace()
            code = 4
            rflag=false            
          }
        }
        case ce: Exception => {
          errmsg = ce.getMessage
          ce.printStackTrace()
          LG.awpLog(ce.toString())
          code = 3
          rflag=false
        }
      }
    }  while( rflag )

    sw.awpLog(f"SPRINTER EXEC time:")
    if( code != 0 ) {
      LG.awpErrMsg(errmsg)
    }
                 
    System.exit(code)
  }
  
}