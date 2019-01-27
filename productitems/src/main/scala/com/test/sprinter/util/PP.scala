package com.test.sprinter.util

//   Time: 2017-01-18

import sys.process._

object PP {
  def getpwd(url:String, user:String):String = {
    val sprinterHome = System.getenv("SPRINTER_SERVER_HOME")
    val getShell=s"$sprinterHome/get_fid_pwd.sh"
    if ( !new java.io.File(getShell).exists) "" else  {
      val runCommand=s"$getShell $url $user"
      val retLine:String = runCommand !!
      val retArray = retLine.trim.split("\\s+")
      if(retArray.size != 2) "" else {
        if(retArray(0).equalsIgnoreCase("ROWROW")) retArray(1) else ""
      } 
    } 
  }
}