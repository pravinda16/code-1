package com.citi.sprinter.or2hive

//  Author: Simon(jy44875) Time: 2017-01-23 ~ 2017-05-16

import com.citi.sprinter.core._

case class ORCC (
    val tabBK:     Long,
    val tabMB:     Long,
    val cpuNN:     Int,
    val blkNN:     Int,
    val vwTabs:    Array[String],
    val tbCols:    Array[String],
    val dtCols:    Array[String],
    val tsCols:    Array[String],
    val depth:     Int,
    val flBkF:     Boolean
);

case class ORC(override val s:SSSC, override val x:SSXC, override val r:SSRC, val orclcc: ORCC) extends SSBC(s,x,r)