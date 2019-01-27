package com.test.sprinter.util

//   Time: 2017-01-24 

class SW {
  var s: Long = System.nanoTime

  def start = {
    s = System.nanoTime
    this
  }
  
  def seconds(): Long = {
    ((System.nanoTime - s)  / 1e9).toLong  
  }
  
  def mk():String = {
    val en = System.nanoTime - s
    val es = en / 1e9
    val em = es / 60
    val ss = f"$es%.2f s $em%.2f m"
    s = System.nanoTime
    ss
  }
  
  def debug(prefix: String, suffix: String = "") = {
    val ss = mk()
    LG.debug(s"$prefix $ss $suffix")
  }

  def awpLog(prefix: String, suffix: String = "") = {
    val ss = mk()
    LG.awpLog(s"$prefix $ss $suffix")
  }

  def info(prefix: String, suffix: String = "") = {
    val ss = mk()
    LG.info(s"$prefix $ss $suffix")
  }
    
  override def toString = "elapsed time: " + (System.nanoTime - s) + " ns"
}

object SW {
  def apply(): SW = new SW
}