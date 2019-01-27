package com.test.sprinter.core

//   Time: 2017-01-18 ~ 2017-02-04

object CTRK {
  private var init: Boolean = false;
  private var endf: Boolean = false;
  
  def inited(): Unit = {
    init = true;
  }
  
  def ended(): Unit = {
    endf = true;
  }
    
  def notInited = !init
  
  def endFlag = endf
}