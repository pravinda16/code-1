package com.test.sprinter.core

//   Time: 2017-01-18 ~ 2017-02-04

import com.test.sprinter.util._

object CUTL {
  var argm: Map[String, String]=null;
  
  def argumentToMap(args: Array[String]): Map[String, String] = {
    val tmpa = args.zipWithIndex.map( r => s"argument ${r._2} : [${r._1}]" )
    LG.info("*** args *** :\n" + tmpa.mkString("\n"))  
    val vlen = args.length
    if (vlen != 4 && vlen != 5) {
      throw new SSE(s"Expected len of argument is 4 or 5, got $vlen, example: cmd runId jobId asOfDate:YYYY-MM-DD [type_1^Cname_1^Bval_1^Atype_2^Cname_2^Bval_2]")
    }
    
    var jobArgs = scala.collection.mutable.Map[String, String]()
    jobArgs.put("sprinterCommand",    args(0))
    jobArgs.put("sprinterRunID",      args(1))
    jobArgs.put("sprinterJobID",      args(2))
    jobArgs.put("sprinterAsOfDate",   args(3))
    
    if (args.length == 5) {
      var arg = args(4)
      if (arg.startsWith("'")) {
        arg = arg.substring(1, arg.length() - 1)
      }
      
      for (s <- arg.split("\001")) {
        val strArr = s.split("\002")
        if (strArr.length != 2) {
          throw new SSE(s"invalid parameter, expected len 2, got ${strArr.length}, value: ${s} [type_1^Cname_1^Bval_1]")
        }

        val nameAndType = strArr(0).replace(" ", "").replace("\t", "")
        val ti = nameAndType.indexOf("\003")
        if (ti <= 0) {
          throw new SSE(s"invalid parameter, missing name or type , value: ${nameAndType} [type_1^Cname_1]")
        }

        val argType  = nameAndType.substring(0, ti)
        val argName  = nameAndType.substring(ti + 1)
        val argValue = strArr(1).replace("\005", " ").replace("\004", "'")

        jobArgs.put(argName, argValue)
        jobArgs.put(argType, argValue)
      }
    }
    val argMap =jobArgs.toMap;  argm=argMap;  argMap
    
  }
  
  def getSoruceDatabaseType(cmd: String): SDB.Type = {
     cmd match {
      case "SPRINTER_ORA2HIVE" => SDB.ORACLE
      case "SPRINTER_TD2HIVE"  => SDB.TERADATA
      case "SPRINTER_NZ2HIVE"  => SDB.NETEZZA
      case "SPRINTER_MM2HIVE"  => SDB.MEMSQL
      case "SPRINTER_POC"      => SDB.HIVE
      case _          =>  throw new SSE("source database not supported, expected SPRINTER_ORA2HIVE/SPRINTER_TD2HIVE/SPRINTER_NZ2HIVE got $cmd")
    }
  }

  def getTargeDatabaseType(cmd: String): SDB.Type = {
     cmd match {
      case "SPRINTER_ORA2HIVE" => SDB.HIVE
      case "SPRINTER_TD2HIVE"  => SDB.HIVE
      case "SPRINTER_NZ2HIVE"  => SDB.HIVE
      case "SPRINTER_MM2HIVE"  => SDB.HIVE
      case "SPRINTER_POC"      => SDB.HIVE
      case _          =>  throw new SSE("target database not supported, expected SPRINTER_ORA2HIVE/SPRINTER_TD2HIVE/SPRINTER_NZ2HIVE got $cmd")
    }
  }
  
  def getMandatoryEnv(name: String): String = {
    val v = System.getenv(name)
    if( v != null && v.length() > 0 ) v else throw new SSE(s"$name not set")
  }
    
  def getOptionalEnv(name: String): Option[String] = {
    val v = System.getenv(name)
    if( v != null && v.length() > 0 ) Some(v) else Option(null)
  }
  
  def getOptionalEnvx(name: String): Option[String] = {
    if (argm.contains(name)) argm.get(name) else getOptionalEnv(name)
  }
  
  def getMandatoryArg(args: Map[String, String], name: String): String = {
    if (args.contains(name)) args.get(name).get else getMandatoryEnv(name)
  }

  def getMandatoryArg2(args: Map[String, String], name: String, envName:String): String = {
    if (args.contains(name)) args.get(name).get else getMandatoryEnv(envName)
  }
  
  def getOptionalArg(args: Map[String, String], name: String): Option[String] = {
    if (args.contains(name)) args.get(name) else getOptionalEnv(name)
  }

  def getOptionalArg2(args: Map[String, String], name: String, envName:String): Option[String] = {
    if (args.contains(name)) args.get(name) else getOptionalEnv(envName)
  }
  
  def expandVar(args: Map[String, String], expr:String): String = {
    var tmp = expr
    for ((kk, vv) <- args) {
     if( tmp.contains( s"#${kk}" ) ) {
       tmp = tmp.replaceAll(s"#${kk}", vv )
     }
    }
    
    val asdt = getMandatoryArg(args, "sprinterAsOfDate")
    tmp = tmp.replaceAll("#asOfDate", asdt)
    tmp
  }
  
  def getOptionalArgWithVar(args: Map[String, String], name: String): Option[String] = {
    val expr = getOptionalArg(args,  name)
    if( expr.isEmpty ) expr else Some( expandVar(args, expr.get) )
  }
  
  def getMandatoryArgWithPrefix(args: Map[String, String], prefix:String, name: String): String = {
    val preval  = getOptionalArg(args, prefix)
    val newname = if( preval.isEmpty ) name else s"${preval.get}_${name}"
    getMandatoryEnv(newname) 
  }
  
  def getOptionalNewCol(args: Map[String, String], name: String): Array[String] = {
    val v = getOptionalArgWithVar(args, name)
    if( v.isEmpty ) Array.empty[String] else v.get.split(";")
  }
  
  def getOptionRetentionPolicy(args: Map[String, String], name: String):Long = {
    val rp = getOptionalArg(args,  "targetTablePartitionRetentionPolicy")
    val rpx = if( rp.isEmpty ) "FOREVER" else rp.get.toUpperCase
    val rpy = rpx.replace("_DAYS", "").replace("DAYS", "").replace("-", "")
    val rpz =   rpy match {
      case "FOREVER" => "-1"
      case "NONE"    => "0"
      case default   => default 
    }
    
    try {
      rpz.toLong
    } catch {
      case ex: Exception => {
        throw new SSE(s"invalid RetentionPolicy, expected NONE/FOREVER/N_DAYS got ${rpx}")
      }
    }
  }
  
  def fixTargetTableLocation(args: Map[String, String], name: String): Option[String] = {
    val hdfs = getMandatoryEnv("SPRINTER_HDFS")
    val tblc = getOptionalArg(args, name)
    if(tblc.isEmpty) tblc else {
       val loc = tblc.get
       val nlc = if( loc.startsWith("hdfs://") ) loc else hdfs + loc
       Some(nlc)
     }
  }

  def getCompressCodec(args: Map[String, String], name: String, envName: String):String = {
    val cc = getOptionalArg2(args,  name, envName)
    val ccx = if( cc.isEmpty ) "gzip" else cc.get.toLowerCase()
    val ccl = Array("uncompressed", "snappy", "gzip", "lzo")
    val idx = ccl.indexOf(ccx)
    if  (idx>=0) ccx else throw new SSE(s"invalid targetTableCompressCodec, expected uncompressed/snappy/gzip/lzo got ${ccx}")
  }
  
  def getTargetTablePartitionDateColumnIndex(args: Map[String, String], name: String): Int = {
    val index = getOptionalArg(args, name)
    if( index.isEmpty ) -1 else index.get.toInt 
  }
    
  def getSprinterConext(argMap: Map[String, String]): SSSC = {
    SSSC(
      getOptionalArg(argMap,            "sourceDatabaseID"),//test run change
      argMap.get("sprinterCommand").get,
      argMap.get("sprinterRunID").get,
      argMap.get("sprinterJobID").get,
      argMap.get("sprinterAsOfDate").get,
      getSoruceDatabaseType(argMap.get("sprinterCommand").get),
      getMandatoryArgWithPrefix(argMap, "sourceDatabaseID",  "SPRINTER_DATASOURCE_JDBC_URL"),
      getMandatoryArgWithPrefix(argMap, "sourceDatabaseID",  "SPRINTER_DATASOURCE_JDBC_USER"),
      Option(null),
      getMandatoryArg(argMap,          "sourceTable"),
      getOptionalArg(argMap,           "sourceTableDataSetID"),
      getTargeDatabaseType(argMap.get( "sprinterCommand").get),
      getMandatoryArg(argMap,           "targetTable"),
      getOptionalArg(argMap,            "targetTablePartitionColumn"),
      getOptionRetentionPolicy(   argMap,            "targetTablePartitionRetentionPolicy"),
      getMandatoryArg2(argMap,          "targetTableFileFormat", "SPRINTER_HIVE_FILE_FORMAT").toUpperCase(),
      fixTargetTableLocation(argMap,    "targetTableLocation"),
      getOptionalEnv("SPRINTER_AWP_DB_JDBC_URL"),
      getOptionalEnv("SPRINTER_AWP_DB_JDBC_USER"),
      Option(null),
      getOptionalArg(argMap,            "sparkParallelDegree"),
      getTargetTablePartitionDateColumnIndex(argMap, "targetTablePartitionDateColumnIndex"))
  }
  
  def VerifySScontxt(ss: SSSC) = {
    if (!ss.sourceTable.contains(".")) {
      throw new SSE(s"invalid sourceTable, expected DB.TAB got ${ss.sourceTable}")
    }
    if (!ss.targetTable.contains(".")) {
      throw new SSE(s"invalid targetTable, expected DB.TAB got ${ss.targetTable}")
    }

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    try {
      val dt = format.parse(ss.sprinterAsOfDate)
    } catch {
      case ex: Exception => {
        throw new SSE(s"invalid date format, expected yyyy-MM-dd got ${ss.sprinterAsOfDate}")
      }
    }

    val suported = ss.targetTableFileFormat match {
      case "PARQUET" => true
      case "AVRO"    => true
      case "CSV"     => true
      case _         => false
    }
    if (!suported) {
      throw new SSE(s"expected file format:PARQUET/CSV/AVRO, got ${ss.targetTableFileFormat}")
    }

    if( !ss.sparkParallelDegree.isEmpty ) {
      try {
        val degree = ss.sparkParallelDegree.get.toLong
      } catch {
        case ex: Exception => {
          throw new SSE(s"invalid sparkParallelDegree, expected int got ${ss.sparkParallelDegree.get}")
        }
      }
    }
  }
  
  def getSprinterExConext(argMap: Map[String, String]): SSXC = {
    val pt  = if( !getOptionalArg(argMap,"targetTablePartitionColumn").isEmpty )  true  else false
    val ls  = if(pt) {
      getOptionalArg(argMap, "sourceTablePartitionLoadingStrategory").getOrElse("INCREMENTAL_LOADING_CNT").toUpperCase 
    }else {
      getOptionalArg(argMap, "sourceTablePartitionLoadingStrategory").getOrElse("FULL_LOADING").toUpperCase  
    }
    
    val x = SSXC(
      getOptionalArgWithVar(argMap,    "sourceTableFilterExpr"),
      getOptionalArg2(argMap,          "sourceTableDisableNoDataException",    "SPRINTER_DISABLE_NO_DATA_EXCEPTIOIN"),
      getOptionalNewCol(argMap,        "sourceTableNewColumnsName"),
      getOptionalNewCol(argMap,        "sourceTableNewColumnsExpr"),
      getOptionalNewCol(argMap,        "sourceTableNewColumnsType"),  
      getOptionalArg2(argMap,          "sourceTableDateFormat",                 "SPRINTER_HIVE_DATE_FORMAT"),
      getOptionalArg2(argMap,          "sourceTableTimestampFormat",            "SPRINTER_HIVE_TIMESTAMP_FORMAT"),
      getOptionalArg2(argMap,          "sourceTableTrimStringColumns",          "SPRINTER_ENABLE_TRIMING_STRING"),
      ls,
      getCompressCodec(argMap,         "targetTableCompressCodec",              "SPRINTER_COMPRESS_CODEC")
      ); 
    
    LG.info(s"targetTablePartitionColumn: $pt sourceTablePartitionLoadingStrategory: ${ls} ")
    x
  }

  def verifyLoadStrategory(ss: SSSC, strategory: String) = {
     if( !strategory.isEmpty )    {
      val ls = strategory.toUpperCase
      val suported =  ls match {
        case "FULL_LOADING"               => true
        case "TRUNCATE_LOADING"           => true
        case "INCREMENTAL_LOADING_CNT"    => true
        case "INCREMENTAL_LOADING_COB"    => true
        case "INCREMENTAL_LOADING_DRV"    => true
        case _                            => false
      }
      
      if (!suported) {
        throw new SSE(s"expected loading strategory: FULL_LOADING/TRUNCATE_LOADING/INCREMENTAL_LOADING_CNT/INCREMENTAL_LOADING_COB/INCREMENTAL_LOADING_DRV, got ${ls}")
      }
    }   
  }
  
  def verifySXcontxt(ss: SSSC, xx: SSXC) = {
    if (xx.sourceTableNewColumnsExpr.length != xx.sourceTableNewColumnsName.length || xx.sourceTableNewColumnsExpr.length != xx.sourceTableNewColumnsType.length) {
      throw new SSE(s"invalid newCol length ${xx.sourceTableNewColumnsExpr.length} ${xx.sourceTableNewColumnsName.length} ${xx.sourceTableNewColumnsType.length}")
    }
  }
    
  def getSparkPWD(ss: SSSC) = {
    if (TL.countMatches(ss.sourceTable, '.') != 1) {
      throw new SSE(s"invalid sourceTable, expected DB.TAB got ${ss.sourceTable}")
    }
    if (TL.countMatches(ss.targetTable, '.') != 1) {
      throw new SSE(s"invalid targetTable, expected DB.TAB got ${ss.targetTable}")
    }

    val pwd = PP.getpwd(ss.sourceDatabaseURL, ss.sourceDatabaseUser)
    if (pwd == null || pwd.length() <= 0) {
      throw new SSE(s"invalid PWP setup url: ${ss.sourceDatabaseURL} user: ${ss.sourceDatabaseUser}")
    }
    ss.sourceDatabasePassword = Some(pwd)

    if (!ss.sourceTableDataSetID.isEmpty) {
      if (ss.awpDatabaseURL.isEmpty || ss.awpDatabaseUser.isEmpty) {
        throw new SSE(s"sourceTableDataSetID defined, but SPRINTER_AWP_DB_JDBC_URL/SPRINTER_AWP_DB_JDBC_URL not set")
      }
      val awpPWD = PP.getpwd(ss.awpDatabaseURL.get, ss.awpDatabaseUser.get)
      if (awpPWD == null || awpPWD.length() <= 0) {
        throw new SSE(s"invalid PWP setup url: ${ss.awpDatabaseURL.get} user: ${ss.awpDatabaseUser.get}")
      }
      ss.awpDatabasePassword = Some(awpPWD)
    }
  }

  def getDriverName(s: SSSC): String = {
    s.sourceDatabaseType match {
      case SDB.ORACLE   => "oracle.jdbc.OracleDriver"
      case SDB.TERADATA => "com.teradata.jdbc.TeraDriver"
      case SDB.NETEZZA  => "org.netezza.Driver"
      case SDB.MEMSQL   => "com.mysql.jdbc.Driver"
      case _            => throw new SSE(s"not supported source database, except:ORACLE/TERADATA/NETEZZA got: ${s.sourceDatabaseType}")
    }
  }
  
   def getOption(s:SSSC, sql: String, fsz: Long): Map[String, String] = {
    val driver = getDriverName(s)
    val oo = Map(
        "driver"             -> driver,
        "url"                -> s.sourceDatabaseURL,
        "fetchsize"          -> fsz.toString,
        "user"               -> s.sourceDatabaseUser,
        "dbtable"            -> sql)

     if ( s.sourceDatabasePassword.get == "_PWD_NA_" ) oo else (oo ++ Map( "password" -> s.sourceDatabasePassword.get) )  
  }
      
   def getXOption(nnn:Int, lll:Int, uuu: Int ):Map[String, String] = {
     val oo = Map(
      "partitionColumn"    -> "SPRINTER_ID",
      "numPartitions"      -> s"$nnn",
      "lowerBound"         -> s"$lll",
      "upperBound"         -> s"$uuu")
    oo
   }
   
   def dumpSSSC(s: SSSC, newtb:String): SSSC= {
      val ns = SSSC(
        s.sourceDatabaseID,//test run change
        s.sprinterCommand                           ,
        s.sprinterRunID                             ,
        s.sprinterJobID                             ,
        s.sprinterAsOfDate                          ,
        s.sourceDatabaseType                        ,
        s.sourceDatabaseURL                         ,
        s.sourceDatabaseUser                        ,
        s.sourceDatabasePassword                    ,
        s.sourceTable                               ,
        s.sourceTableDataSetID                      ,
        s.targetDatabaseType                        ,
        newtb                                       ,
        Option(null)                                ,
        -1                                          ,
        s.targetTableFileFormat                     ,
        s.targetTableLocation                       ,
        s.awpDatabaseURL                            ,
        s.awpDatabaseUser                           ,
        s.awpDatabasePassword                       ,
        s.sparkParallelDegree                       ,
        s.targetTablePartitionDateColumnIndex       )
     ns
   }

   def dumpSSXC(x: SSXC, newW:String): SSXC= {
      val nw = if (x.sourceTableFilterExpr.isEmpty) s" $newW" else s" ${x.sourceTableFilterExpr.get} AND ( $newW ) "     
      val nx = SSXC(
            Some(nw),
            x.sourceTableDisableNoDataException,
            x.sourceTableNewColumnsName , 
            x.sourceTableNewColumnsExpr , 
            x.sourceTableNewColumnsType ,
            x.sourceTableDateFormat,
            x.sourceTableTimestampFormat,
            x.sourceTableTrimStringColumns,
            x.targetTableLoadingStrategory,
            x.targetTablecompressCodec)
     nx
   }
}