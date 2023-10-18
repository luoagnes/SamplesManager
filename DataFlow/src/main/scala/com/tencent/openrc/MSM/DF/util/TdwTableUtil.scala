package com.tencent.openrc.MSM.DF.util

import com.tencent.openrc.MSM.DF.util.TimeUtil.getPeriodDay
import com.tencent.tdw.spark.toolkit.tdw.{TDWProvider, TDWSQLProvider, TDWUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Date

object TdwTableUtil {

  val user = "tdw_youngguo"
  val passwd = "rfgdsl/13"
  def sparkInit(appName: String = "DefaultApp", sparkConf: SparkConf = new SparkConf()) = {
    val sparkSession = SparkSession.builder.appName(appName).config(sparkConf).getOrCreate
    sparkSession
  }

  def getPartitionNum(sc: SparkContext, times: Int = 2) = {
    val worker_num = sc.getConf.get("spark.executor.instances", "600").toInt *
      sc.getConf.get("spark.executor.cores", "4").toInt
    worker_num * times
  }

  def getLatestData(sc: SparkContext, period: Int = 1, db:String, table: String) = {
    val partitions = TdwTableUtil.getLatestPartitions(db + "::" + table, period)
    println("Acquired partitions %s from %s::%s ,".format(partitions, db, table))
    val TdwProvider = new TDWProvider(sc, user, passwd, db)
    val data = TdwProvider.table(table, partitions)
    data
  }

  def getLatestDataDF(ss: SparkSession, period: Int = 1, db:String, table: String) = {
    val partitions = TdwTableUtil.getLatestPartitions(db + "::" + table, period)
    println("Acquired partitions %s from %s::%s ,".format(partitions, db, table))
    val TdwProvider = new TDWSQLProvider(ss, user, passwd, db)
    val data = TdwProvider.table(table, partitions)
    data
  }

  // 把结果写到tdw表中
  def saveToTdwTable(dataResult: DataFrame, tdwDBName: String, tdwTable: String,
                     routineDay: String, partition_type:String = "List", overwrite: Boolean = false) = {
    val beginTime = new Date().getTime / 1000
    println(s"\nsaveToTdwTable Begin, Time is:${beginTime}!")

    val ss = SparkSession.builder().getOrCreate()

    val tdwUtil = new TDWUtil(user, passwd, tdwDBName)
    val partition = "p_" + routineDay
    // 不存在该分区则创建
    if (!tdwUtil.partitionExist(tdwTable, partition)) {
      if (partition_type.equals("List")){
        tdwUtil.createListPartition(tdwTable, partition, routineDay)
      } else if (partition_type.equals("Range")){
        val tomorrow = getPeriodDay(routineDay, 1)
        tdwUtil.createRangePartition(tdwTable, partition, tomorrow)
      }
    }

    val tdw = new TDWSQLProvider(ss, user, passwd, tdwDBName)
    tdw.saveToTable(dataResult, tdwTable, partition, overwrite = overwrite)

    val endTime = new Date().getTime / 1000
    println(s"\nsaveToTdwTable End, Time is:${endTime}, Run Time is:${(endTime - beginTime) / 60.0} min!")
  }

  def saveRddToTdw(tablePath: String, res:RDD[Array[String]], partition: String,
                   overwrite:Boolean, sc: SparkContext): Unit={
    val tmp = tablePath.split("::")
    assert(tmp.length == 2)
    val database = tmp(0)
    val table = tmp(1)
    val tdwSaveClient = new TDWProvider(sc, user, passwd, database)

    val cnt = res.count()

    if(cnt < 10000000) {
      res.coalesce(50)
    } else {
      if(res.getNumPartitions > 300) {
        res.coalesce(300)
      }
    }

    if(partition.eq(null)){
      tdwSaveClient.saveToTable(res, table, overwrite = overwrite)
    } else {
      if (overwrite) {
        val tdwUtil = new TDWUtil(user, passwd, database)
        tdwUtil.dropPartition(table, partition)
        println(s"Overwrite drop table ${tablePath}")
        tdwUtil.createListPartition(table, partition, partition.split("_")(1))
      }
      tdwSaveClient.saveToTable(res, table, partition, overwrite = overwrite)
    }
  }

  def chekPartitionExist(tablePath: String, partiton: String): Boolean={
    val tmp = tablePath.split("::")
    assert(tmp.length == 2)
    val database = tmp(0)
    val table = tmp(1)
    val tdwUtil = new TDWUtil(user, passwd, database)
    val p1Exist = tdwUtil.partitionExist(table, partiton)
    p1Exist
  }

  def getPartitionRowcount(DB:String, tablePath: String, partitons: Array[String]): Long={
    val database = DB
    val table = tablePath
    val tdwUtil = new TDWUtil(user, passwd, database)
    tdwUtil.showRowCount(table, partitons)
  }

  def getLatestAvailablePartitionDate(DB:String, tablePath:String, routineDay:String): String ={
    var day = routineDay
    var partition = "p_" + routineDay
    var count = getPartitionRowcount(DB, tablePath, Array(partition))
    while (count <= 0){
      day = getPeriodDay(day, -1)
      partition = "p_" + day
      count = getPartitionRowcount(DB, tablePath, Array(partition))
    }
    day
  }

  def getLatestPartitions(tablePath: String, numPartitions: Int): Seq[String]= {
    val tmp = tablePath.split("::")
    assert(tmp.length == 2)
    val database = tmp(0)
    val table = tmp(1)
    val tdwUtil = new TDWUtil(user, passwd, database)
    val tbInfo = tdwUtil.getTableInfo(table)
    val partitions: Seq[String] = tbInfo.partitions.map(line => (line.level, line.name))
      .filter(_._1 == 0).map(_._2).sorted.reverse
    partitions.take(numPartitions)
  }

  def getLatestPartition(tablePath: String): String={
    val tmp = tablePath.split("::")
    assert(tmp.length == 2)
    val database = tmp(0)
    val table = tmp(1)
    val tdwUtil = new TDWUtil(user, passwd, database)
    val tbInfo = tdwUtil.getTableInfo(table)
    val partitions: Seq[String] = tbInfo.partitions.map(line=>(line.level, line.name))
      .filter(_._1 == 0).map(_._2).sorted.reverse
    partitions.take(1).mkString
    //      .slice(0, 10).toArray
    //    //        println(partitions.mkString("\t"))
    //    var maxCnt = 0L
    //    var latestPartitionDate = 0
    //    for(partition <- partitions) {
    //      val partitionDate = partition.split("_")(1).toInt
    //      val cnt = tdwUtil.showRowCount(table, Array(partition))
    //      if(partitionDate > latestPartitionDate && cnt >= maxCnt) {
    //        latestPartitionDate = partitionDate
    //        maxCnt = cnt
    //      }
    //    }
    //    assert(latestPartitionDate > 0 && maxCnt > 0)
    //    println(s"Tbale=$tablePath\tlatest=$latestPartitionDate\tcount=$maxCnt")
    //    "p_" + latestPartitionDate
  }

  def getRddFromTdw(tablePath: String, partitions: Array[String],
                    ValueColNames: Array[String], sc: SparkContext): RDD[Array[String]]={
    val tmp = tablePath.split("::")

    assert(tmp.length == 2)
    val database = tmp(0)
    val table = tmp(1)
    var adDailyDataRdd: RDD[Array[String]] = null
    val tdwRddClient = new TDWProvider(sc, user, passwd, database)
    if(partitions.eq(null)) {
      adDailyDataRdd = tdwRddClient.table(table)
    }
    else {
      adDailyDataRdd = tdwRddClient.table(table, partitions)
    }
    //        adDailyDataRdd.take(10).map(_.mkString(" | ")).foreach(println(_))
    val tdwUtils = new TDWUtil(user, passwd, database)

    val tbInfos = tdwUtils.getTableInfo(table)
    val colName = tbInfos.colNames.toArray
    val colNameIdxMap = colName.zipWithIndex.toMap
    val colNmaeIdxMapBc: Broadcast[Map[String, Int]] = sc.broadcast(colNameIdxMap)

    val res = adDailyDataRdd.map(line=>{
      val data = ValueColNames.map(colNmaeIdxMapBc.value.getOrElse(_, 999999)).map(line(_))
      data
    })
    res
  }


  def getRddFromTdw(tablePath: String, partitions: Array[String],
                    keyColName: String, ValueColNames: Array[String], sc: SparkContext): RDD[(String, Array[String])]={
    val tmp = tablePath.split("::")
    println(tmp.mkString("|"))
    assert(tmp.length == 2)
    val database = tmp(0)
    val table = tmp(1)
    var adDailyDataRdd: RDD[Array[String]] = null
    val tdwRddClient = new TDWProvider(sc, user, passwd, database)
    if(partitions.eq(null)) {
      adDailyDataRdd = tdwRddClient.table(table)
    }
    else {
      adDailyDataRdd = tdwRddClient.table(table, partitions)
    }

    val tdwUtils = new TDWUtil(user, passwd, database)
    val tbInfos = tdwUtils.getTableInfo(table)
    val colName = tbInfos.colNames.toArray
    val colNameIdxMap = colName.zipWithIndex.toMap
    val colNmaeIdxMapBc: Broadcast[Map[String, Int]] = sc.broadcast(colNameIdxMap)

    val res = adDailyDataRdd.map(line=>{
      val data = ValueColNames.map(colNmaeIdxMapBc.value.getOrElse(_, 999999)).map(line(_))
      val key = line(colNmaeIdxMapBc.value.getOrElse(keyColName, 999999))
      (key, data)
    })
    res
  }

}
