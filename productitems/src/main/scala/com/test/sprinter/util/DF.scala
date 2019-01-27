package com.test.sprinter.util

//   Time: 2017-01-24 ~ 2017-04-21

import com.test.sprinter.core._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext

object DF {
  def ogTCI(tci: Array[STCI], ptk:String): Array[STCI] = {
    tci.filter { r => r.n != ptk.toUpperCase() } ++ tci.filter { r => r.n == ptk.toUpperCase() }
  }

  def ogTCIx(tci: Array[STCI], ptk:Array[String]): Array[STCI] = {
    val ptkUP=ptk.map( r => r.toUpperCase() )
    tci.filter { r => !ptkUP.contains(r.n) } ++ tci.filter { r => ptkUP.contains(r.n)  }
  }
    
  def getPKT(tci: Array[STCI], ptk:String): String = {
    tci.filter { r => r.n == ptk.toUpperCase() }.map( r => r.t ).mkString("").toUpperCase()
  }

  def getPKTx(tci: Array[STCI], ptk:Array[String]): Array[String] = {
    val ptkUP=ptk.map( r => r.toUpperCase() )
    tci.filter { r => ptkUP.contains(r.n) }.map( r => r.t.toUpperCase() )
  }
  
  def getTCI(df: DataFrame, ssc: HiveContext): Array[STCI] = {
    df.registerTempTable("stci")
    val dc = ssc.sql("describe stci")
    val tci = dc.collect().map { row =>
      STCI(row.getString(0).toUpperCase, row.getString(1).toUpperCase)
    }
    tci
  }
 
  def save(df: DataFrame, tbpath: String, flfmt: String, dtcols:Array[String] = Array.empty[String]): Unit = {
    val lc = if( dtcols.length == 0) {
      df.columns.filter(r => r.toUpperCase != "SPRINTER_ID").map(name => col(name).as(s"${name.toLowerCase}"))
    }else {
      df.columns.filter(r => dtcols.contains(r) && r.toUpperCase != "SPRINTER_ID" ).map(name => col(name).as(s"${name.toLowerCase}"))
    }
    
    val ff = df.select(lc: _*)
    if (flfmt == "AVRO") {
      ff.write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(tbpath)
    } else if (flfmt == "PARQUET") {
      ff.write.mode(SaveMode.Overwrite).parquet(tbpath)
    } else if (flfmt == "CSV") {      
       ff.write.format("com.databricks.spark.csv")
         .mode(SaveMode.Overwrite)
         .option("header", "false")
         .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .option("delimiter", "\001")
         .save(tbpath)
    }
    
    ff.unpersist();
  }
    
  def savex(df: DataFrame, tbpath: String, flfmt: String, tci: Array[STCI], ptc:String ): Unit = {
    val dtcols = tci.map { r => r.n }
    val lc = if( dtcols.length == 0) {
      df.columns.filter(r => r.toUpperCase != "SPRINTER_ID").map(name => col(name).as(s"${name.toLowerCase}"))
    }else {
      df.columns.filter(r => dtcols.contains(r) && r.toUpperCase != "SPRINTER_ID" ).map(name => col(name).as(s"${name.toLowerCase}"))
    }
    
    val ff = df.select(lc: _*)    
    if (flfmt == "AVRO") {
      ff.write.partitionBy(ptc).format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(tbpath)
    } else if (flfmt == "PARQUET") {
      ff.write.partitionBy(ptc).mode(SaveMode.Overwrite).parquet(tbpath)
    } else if (flfmt == "CSV") {      
       ff.write.partitionBy(ptc).format("com.databricks.spark.csv")
         .mode(SaveMode.Overwrite)
         .option("header", "false")
         .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .option("delimiter", "\001")
         .save(tbpath)
    }
    
    ff.unpersist();
  }
  
  def save(df: DataFrame, tbpath: String, flfmt: String, tci: Array[STCI]): Unit = {
    val dtcols = tci.map { r => r.n.toLowerCase() }
    save(df, tbpath, flfmt, dtcols)
  }
  
  def getCnt(df: DataFrame): Long = {
    val tt = df.schema.fields(0).dataType.toString()
    val tn = if(tt.contains("Int")) "int" else {
      if(tt.contains("Long")) "long" else "decimal"
    }
    
    tn match {
      case "int"    => df.first.getInt(0).toLong
      case "long"   => df.first.getLong(0)
      case "decimal" => df.first.getDecimal(0).longValue()
    }
  }
}