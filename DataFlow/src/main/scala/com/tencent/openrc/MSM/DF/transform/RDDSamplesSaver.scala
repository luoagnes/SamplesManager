package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.CS.util.IO.{printlog, isExistsHdfs, rmHdfs}
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory



class RDDSamplesSaver(id: String) extends UnaryNode[RDD[String], Unit](id) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val sc = SparkRegistry.sc

  override def doExecute(input: RDD[String]): Unit = {
    val outputRootPath = getPropertyOrThrow("outputPath")
    printlog("output root path: " + outputRootPath)

    if(isExistsHdfs(outputRootPath) ) rmHdfs(outputRootPath)

    input.saveAsTextFile(outputRootPath)

  }
}
