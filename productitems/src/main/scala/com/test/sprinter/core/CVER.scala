package com.test.sprinter.core

//   Time: 2017-02-24 ~ 

/*
 * ==========================================================================================================================
 *  1.2.0 2016-??-??   rewrite whole code in ETLite and rename to sprinter 
 *  1.2.1 2017-01-10   improve non partition table performance and update partition key type to time stamp
 *  1.2.3 2017-02-20   add schema reconciliation and retention policy support.
 *  1.2.4 2017-02-24   improve partition table performance and fix all found bugs
 *  1.2.5 2017-03-03   sprinter server bug fix and TD AWP support
 *  1.2.6 2017-03-08   change sprinter directory/installation to meet EAP standard and support CNT incremental loading.
 *  1.2.7 2017-03-16   
 *                     parallel degree/chunk update, enhance CNT, calculate driver memory
 *                     nfs file watch work around
 *                     add test
 *                     
 *  1.2.8  2017-03-26  improve partition table loading performance
 *  1.2.9  2017-03-31  
 *                      support '#asOfDate' in filterExpr
 *                      send load count to portal
 *                      add multiply db support
 *                      fix database name in teradata
 *  1.3.10 2017-04-23
 *                      try to fix trigger file pickup issue
 *                      send started status to AWP
 *                      add default FS before target table location
 *                      td column name contain space
 *                      support sub partition
 *  1.3.11 2017-04-27
 *                      change partition type to string and convert date/time stamp to string
 *  1.3.12 2017-05-05
 *  										use dedicated data set convert
 *  1.3.13 2017-05-12
 *  										table schema recon & fix decimal & date/time stamp issue
 *                      fix fake failure
 *                      update sprinter installation location.
 *  1.3.14  2017-05-16
 *  
 *         						  do not purge data for data pull with filter express
 *         							bug fix for table restore
 *  1.3.15  2017-05-19
 *         							refactor oracle2Hive:
 *         						  	change time stamp partition to string partition for Oracle2Hive
 *         	            	add data type recon for Oracle2Hive
 *         								add data pull from oracle view
 *         								support multiply partition key for Oracle2Hive
 *         						    
 *         								add date/time stamp format in sprinter profile
 *         						  add float support in td2hive
 *         							add testing cases for multiply partition key
 * 1.4.16   2017-05-23
 * 											netezza
 * 1.4.17   2017-05-26
 *                      refactor big table loading
 * 1.4.18   2017-05-29
 *                      update row count logic
 *                      tune sprinter performance
 *                      support yarn queue
 * 1.4.19   2017-06-05
 *                      add MNH for teradata and netezza
 *                      add file id
 * 1.5.20   2017-06-03
 * 											memsql
 * 
 * 1.5.21~1.5.23        2017-07-11
 *                      support date column as the first partition column.
 *                      move file id to hdfs
 *                      
 * 1.5.24   2017-07-19
 *                      bug fix for Teradata
 * 1.5.25   2017-07-27
 *                      bug fix for expired partition clean up
 * 1.5.26   2017-08-01
 * 											bug fix for Terdata
 * 1.5.27   2017-08-28  2017-09-07
 *                      bug fix for Teradata
 * 1.5.28   2017-09-20  
 *                      support trim space for string columns in Teradata
 *          2017-10-10
 *          			      minor fix
 * 1.5.29   2017-10-26
 *                      memsql bugfix  
 * 1.5.30   2017-11-20
 * 											memsql purge and oracle CLOB support
 * 1.5.31   2017-12-25
 *                      add targetTableCompressCodec support  
 * 1.5.32   2018-02-01
 *                      support parallel in DSMT
 * 1.5.33   2018-03-09
 *                      last version by Simon
 * 1.5.34   2018-09-21
 *                      Sprinter UPDATE Support added by test run
 * 1.5.35   2018-10-05
 *                      Added "spark.executor.instances" configurable from pre-req :=> added by test run
 * 1.5.36   2019-01-11
 *                      Support provided for spark 2.x version :=> added by test run
 * ========================================================================================================================== 
 * 
 */

object CVER {
  def getBanner():String = {
    val banner = """
Welcome to

  _   _   _   _   _   _   _   _  
 / \ / \ / \ / \ / \ / \ / \ / \ 
( S | p | r | i | n | t | e | r )
 \_/ \_/ \_/ \_/ \_/ \_/ \_/ \_/   version 1.5.36 2019-01-11 10:00
 

"""
  banner
  }
}