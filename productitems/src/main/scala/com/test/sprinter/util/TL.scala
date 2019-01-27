package com.test.sprinter.util

//   Time: 2017-01-18

import org.apache.spark.sql.Row
import com.test.sprinter.core._
import java.text.SimpleDateFormat

object TL {

  def fTbCol(r: STCI)(c: String = "SPRINTER_ID"): Boolean = {
    r.n != c
  }

  def fTbCols(r: STCI)(cs: Array[String]): Boolean = {
    !cs.contains(r.n)
  }

  def getTFD(tbfmt: String): String = {
    tbfmt.toUpperCase match {
      case "PARQUET" => "STORED AS PARQUET"
      case "AVRO"    => "STORED AS AVRO"
      case "CSV"     => "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' "
    }
  }

  def fixAVRO(dt: String, flfmt: String): String = {
    if (flfmt != "AVRO") dt else {
      if (dt == "timestamp") "bigint" else dt //bigint
    }
  }
  
  def fixName(n: String): String = {
    if( n.contains(" ")) "\"" + n + "\"" else n
  }
  
  def countMatches(lstr:String, sstr:Char) = lstr.count(_ == sstr)

  def findTab(a:Array[Int], b:Array[Int], mb: Int ) : Int = {
    val ii = a.zipWithIndex.filter{ case(v,i) => v > mb }(0)._2
    b(ii)
  }
 
  def getOrFindTable(key:String, a:Array[Int], b:Array[Int], mb: Int ): Int = {
    val v = findTab(a,b,mb).toString
    CUTL.getOptionalEnvx(key).getOrElse(v).toInt
  }

  def cross1(arr: Array[Int], one: Int): Array[(Int, Int)] = {
    arr.map( r => (r,one) ).toArray
  }
  
  def cross(a: Array[Int], b: Array[Int]) : Array[(Int, Int)] = {
    b.map( r => cross1(a, r) ).flatten.sortWith((e1, e2) => (e1._2 < e2._2))
  }
  
  def genid(n:Int, a:Array[Int]): Array[Int] = {
    Seq.fill((n/a.length)+1)(a).flatten.slice(0,n).toArray
  }
  
  def genpk(n:Int, d:Int): Array[Int] = {
    (1 to n/d+1).map( r => Seq.fill(d)(r) ).flatten.slice(0,n).toArray
  }

  def tryParse(fmt:SimpleDateFormat, dt:String, defaultv: String = "9999-01-01") = {
    try {
      fmt.parse( dt )
    } catch {
      case th: Throwable => {
      fmt.parse(defaultv)
      }
    }
  }
  
}