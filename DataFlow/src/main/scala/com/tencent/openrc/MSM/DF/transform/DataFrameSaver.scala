package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.CS.util.IO.{isExistsHdfs, printlog, rmHdfs, saveAsText,saveTFRecord}
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class DataFrameSaver (id: String) extends UnaryNode[DataFrame, Unit](id) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val ss = SparkRegistry.spark

  override def doExecute(input: DataFrame): Unit = {
    val outputRootPath = getPropertyOrThrow("outputPath")
    val data_type = getPropertyOrThrow("save_type")
    printlog("output root path: " + outputRootPath)
    printlog("data_type: " + data_type)
    if(isExistsHdfs(outputRootPath) ) rmHdfs(outputRootPath)

    data_type match{
      case "tfrecord" => saveTFRecord(ss, outputRootPath, input)
      case "text" =>saveAsText(input,outputRootPath)
    }
  }
}
