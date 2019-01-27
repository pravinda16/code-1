package com.test.sprinter.util

//   Time: 2017-01-26 ~ 2017-05-17

import com.test.sprinter.core.STCI

object HT {
  //class=A/cob_dt=2017-01-01
  def getPTEFromPTS(pts:String):String = {
    val plist = pts.split("/").map { r =>
      val t=r.split("=")
      val k=t(0)
      val v=t(1)
      s"$k='$v'"
    }
    plist.mkString(",")
  }

  def getYMDFromPTS(ptk: String, pts: String): String = {
    val tmpstr = s"${ptk.toLowerCase}="
    val tmpArr = pts.split("/").map { r =>
      if (r.contains(tmpstr)) r.replace(tmpstr, "") else ""
    }.filter(!_.isEmpty)

    val ptvYMD=tmpArr.mkString("")
    ptvYMD
  }
}
