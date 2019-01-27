package com.test.sprinter.core

//   Time: 2017-01-19 ~ 2017-03-13

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SDB extends Enumeration {
  type Type = Value
  val ORACLE, TERADATA, NETEZZA, HIVE, MEMSQL = Value
}

case class SSSC(
    sourceDatabaseID:                          Option[String],//test run change
    sprinterCommand:                           String,    
    sprinterRunID:                             String,
    sprinterJobID:                             String,
    sprinterAsOfDate:                          String,
    sourceDatabaseType:                        SDB.Type,
    sourceDatabaseURL:                         String,
    sourceDatabaseUser:                        String,
    var sourceDatabasePassword:                Option[String],
    sourceTable:                               String,
    sourceTableDataSetID:                      Option[String],
    targetDatabaseType:                        SDB.Type,
    targetTable:                               String,
    targetTablePartitionColumn:                Option[String],  
    targetTablePartitionRetentionPolicy:       Long,  
    targetTableFileFormat:                     String,
    targetTableLocation:                       Option[String],
    awpDatabaseURL:                            Option[String],
    awpDatabaseUser:                           Option[String],
    var awpDatabasePassword:                   Option[String],
    sparkParallelDegree:                       Option[String],
    targetTablePartitionDateColumnIndex:       Int
);

case class SSXC(
    sourceTableFilterExpr:                     Option[String],
    sourceTableDisableNoDataException:         Option[String],
    sourceTableNewColumnsName:                 Array[String],
    sourceTableNewColumnsExpr:                 Array[String],
    sourceTableNewColumnsType:                 Array[String],
    sourceTableDateFormat:                     Option[String],
    sourceTableTimestampFormat:                Option[String],
    sourceTableTrimStringColumns:              Option[String],
    targetTableLoadingStrategory:              String,
    targetTablecompressCodec:                  String
);

case class SSRC(
    val sc:                                   SparkContext,       
    val sparkSSC:                             HiveContext,
    val sparkMaxParallelDegree:               Int,
    val sparkDriverMemory:                    Int,
    val sparkExecutorCores:                   Int,
    val sparkExecutorMemory:                  Int, 
    val sourceTableJDBCFetchSize:             Int,  
    val ideaParallelDegree:                   Int,
    val sparkParallelDegree:                  Int,
    val sparkCNum:                            Int,
    val sparkSNum:                            Int
);


class SSBC(val s:SSSC, val x:SSXC, val r:SSRC);
case class SSE(emsg:String)  extends Exception

trait TETLS {
  def r() 
}

trait TETLP {
  def p(): SSBC
}

trait TETLM {
  def r(): Unit
}

case class STCI(val n:String, val t:String)
case class STCR(val succ:   Boolean, val inHiveOrSrc: String, val inDSMT:   String ); //2017-02-08
