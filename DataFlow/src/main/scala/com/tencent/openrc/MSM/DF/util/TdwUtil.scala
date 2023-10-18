package com.tencent.openrc.MSM.DF.util

import com.tencent.tdw.spark.toolkit.tdw.TDWUtil

object TdwUtil {


  def getLastNPartition(db: String,
                        tbl: String,
                        tdw_user: String,
                        tdw_passwd: String,
                        partition_num: Int): Array[String] = {
    val tdw_util =
      if (tdw_user != null && tdw_passwd != null &&
        tdw_user.nonEmpty && tdw_passwd.nonEmpty)
        new TDWUtil(tdw_user, tdw_passwd, db)
      else
        new TDWUtil(db)
    val parts = tdw_util
      .getTableInfo(tbl)
      .partitions
      .sortWith(_.name > _.name)
      .map(_.name)
      .toArray


    val finalPart = parts
      .map(part => {
        val size = try {
          tdw_util.tableSize(tbl, Array(part))
        } catch {
          case _: Throwable => 0L
        }
        (part, size)
      })
      .filter(line => line._2 > 0 && line._1 != "default")
      .map(line => line._1)

    if(tbl == "dtl_all_appid_fetas_wide_d"){
      finalPart.slice(1, finalPart.length).take(partition_num)
    }else{
      finalPart.take(partition_num)
    }
  }

  def getLastPartition(db: String,
                       tbl: String,
                       tdw_user: String,
                       tdw_passwd: String) = {
    val parts = getLastNPartition(db, tbl, tdw_user, tdw_passwd, 1)
    parts.head
  }
}
