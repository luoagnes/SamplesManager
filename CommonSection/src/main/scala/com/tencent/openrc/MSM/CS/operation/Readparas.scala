package com.tencent.openrc.MSM.CS.operation


import org.apache.spark.sql.SparkSession
import com.tencent.openrc.MSM.CS.conf.Configure
import com.tencent.openrc.MSM.CS.util.IO.getConfigureFileInputStream

object Readparas {

  /**
   * read the whole configure xml file
   *
   *
   * @param ss: org.apache.spark.sql.SparkSession
   * @param configure_path: the path of configure xml file
   */
  def readWholeConfigure(ss:SparkSession, configure_path:String): Configure = {
    val configure = new Configure
    configure.load(getConfigureFileInputStream(ss, configure_path))
    configure
  }


}
