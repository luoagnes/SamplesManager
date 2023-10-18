package com.tencent.openrc.MSM.DF.transform
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import com.tencent.openrc.MSM.DF.util.TdwTableUtil.getLatestPartitions
import com.tencent.openrc.MSM.DF.util.TimeUtil.getPeriodDay
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import com.tencent.openrc.MSM.DF.util.TdwUtil
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

/**
 * 此节点读取tdw数据转变成DataFrame
 */
class TdwLoader(id: String) extends UnaryNode[Unit, DataFrame](id) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def doExecute(input: Unit): DataFrame = {
    val tdw_table = getPropertyOrThrow("tdw_table")
    val tdw_username = getPropertyOrThrow("tdw_username")
    val tdw_password = getPropertyOrThrow("tdw_userpassword")
    val source = getPropertyOrThrow("source")
    println("TdwLoader" +
      " source: " + source)

    val tdw_db = tdw_table.split("::")(0)
    val tdw_tb = tdw_table.split("::")(1)
    val tdwProvider = new TDWSQLProvider(SparkRegistry.spark, tdw_username, tdw_password, tdw_db)

    var tdwPartition = Seq[String]()
    var partition_num = 0
    var datetime = ""
    var unitDay = 0
    var nday = 0

    //  新增数据补录逻辑,oneside_adx数据很大需要preSample
    if(source == "preSample") {
      partition_num = getPropertyOrThrow("partition_num").toInt
      datetime = getPropertyOrThrow("dateTime")
      unitDay = getPropertyOrThrow("unitDay").toInt
      nday = getPropertyOrThrow("nday").toInt

      val leftdate = getPeriodDay(datetime, 0 - (nday + 1) * unitDay * 3)
      datetime = getPeriodDay(datetime, 0 - nday * unitDay * 3)
      var nPartitionAgo = getLatestPartitions(tdw_table, 230)
      println("nPartitionAgo: " + nPartitionAgo.mkString(","))

      nPartitionAgo = nPartitionAgo.filter(part=>part.split("_").length == 2)
        .filter(part=>part.split("_")(1) <= datetime
          && part.split("_")(1) > leftdate)

      nPartitionAgo = nPartitionAgo.sorted.reverse.take(unitDay)
      println("used partitions for source data:")
      tdwPartition = nPartitionAgo
    } else {
      tdwPartition = TdwUtil.getLastNPartition(tdw_db, tdw_tb, tdw_username, tdw_password, partition_num).toSeq
    }

    println("latest partition:" + tdwPartition.mkString(","))
    if (getProperty("partitions").isDefined) {
      tdwPartition = getProperty("partitions").get.split(";")
    }
    var resDF: DataFrame = tdwProvider.table(tdw_tb, tdwPartition)
    resDF
  }

}
