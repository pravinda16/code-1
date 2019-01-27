package com.test.sprinter.util

//   Time: 2017-08-28

import com.test.sprinter.core._
import java.security.MessageDigest
import org.apache.spark.sql.DataFrame

object LG {
  def md5(s: String): String = {
    MessageDigest.getInstance("MD5").digest(s.getBytes).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}.toUpperCase()
  }

  def awpLog(msg: String): Unit = {
    print(s"\n\nSPRINTER_BEGIN\n$msg\nSPRINTER_END\n\n")
    Console.flush()
  }

  def awpStat(msg: String): Unit = {
    print(s"\n\n\nAWP_STAT_ROWROW: $msg\n\n\n")
    Console.flush()
  }
  
  def awpFileIds(msg: String): Unit = {
    val fixedMsg= msg.replaceAll(",+", ",")
    print(s"\n\n\nAWP_FLID_ROWROW: $fixedMsg\n\n\n")
    Console.flush()
  }
  
  def awpErrMsg(msg: String): Unit = {
    val fixedMsg= msg.replaceAll("\n", " ")
    print(s"\n\n\nAWP_EMSG_ROWROW: $fixedMsg\n\n\n")
    Console.flush()
  }
  
  def log(tag:String, message:String): Unit = {
    println(s"\n\n$tag $message\n\n");
  }
  
  def debug(message: String): Unit = {
    if (inDebug()) {
      log("DEBUG",message)
    }
  }
 
  def showdf(df:DataFrame): Unit = {
     if (inDebug()) {
       df.show(10, false)
     }
  }

  def warn(message: String): Unit = {
    log("WARN",message)
  }
    
  def info(message: String): Unit = {
    log("INFO",message)
  }
    
  def error(err: String): Unit = {
    awpLog(err)
    throw new SSE(err)
  }

  def logSparkOption(option: Map[String, String]): Unit = {
    if (inDebug()) {
      println(s"\n\n\n ***** sprinter option: ******")
      option.filterKeys(_ != "password").foreach(println)
      println()
    }
    
  }
  
  def getv(n:String):String = {
    val v = CUTL.getOptionalArg(CUTL.argm, "SPRINTER_DEBUG_ON"); if(v.isEmpty) "N" else(md5(v.get))
  }
    
  def inDebug(): Boolean = getv("SPRINTER_DEBUG_ON") == "672322D1C333A2CFA9278B7DFCEA77ED"
  def doCount(): Boolean = getv("SPRINTER_COUNT_ON") == "672322D1C333A2CFA9278B7DFCEA77ED"
  def inTest():  Boolean = getv("SPRINTER_TEST_ON")  == "672322D1C333A2CFA9278B7DFCEA77ED"
}