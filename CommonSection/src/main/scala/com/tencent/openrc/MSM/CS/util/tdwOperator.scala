package com.tencent.openrc.MSM.CS.util


import com.tencent.dp.util.IoUtil

object tdwOperator {
//  def getLatestPartitions(tablePath: String, numPartitions: Int): Seq[String] = {
//    val tmp = tablePath.split("::")
//    assert(tmp.length == 2)
//    val database = tmp(0)
//    val table = tmp(1)
//    val tdwUtil = new TDWUtil(user, passwd, database)
//    val tbInfo = tdwUtil.getTableInfo(table)
//    val partitions: Seq[String] = tbInfo.partitions.map(line => (line.level, line.name))
//      .filter(_._1 == 0).map(_._2).sorted.reverse
//    partitions.take(numPartitions)
//  }

}
