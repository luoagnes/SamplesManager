package com.tencent.openrc.MSM.DF.util

import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.SparkContext


object DataCheck {
  def checkReady(
                  sc: SparkContext,
                  inputDonePaths: Array[String],
                  inputDonePathNum: Int): Unit = {
    inputDonePaths.foreach(inputDonePath => {

      val log = LoggerFactory.getLogger(this.getClass)
      val doneFilePath = new Path(inputDonePath)
      val fs = doneFilePath.getFileSystem(sc.hadoopConfiguration)
      // 这里调用globStatus是为了满足输入目录存在通配符的情况, 一般情况下expectedNum为1，如果输入目录为通配符
      // 则需要指定输入目录下有多少个success文件

      var fileStatuses: Array[FileStatus] = Array()
      var actualNum = 0
      var continue = true
      while (continue) {
        fileStatuses = fs.globStatus(doneFilePath)
        actualNum =
          if (fileStatuses == null) {
            0
          } else {
            fileStatuses = fileStatuses.filter(fileStatus => {
              !fileStatus.getPath.toString.contains("_rocksflow_tmp_running")
            })
            fileStatuses.length
          }
        continue = actualNum != inputDonePathNum
        if (continue) {
          log.warn(
            s"""waiting for $inputDonePath, inputDonePathNum = $inputDonePathNum,""" +
              s" actualNum = $actualNum")
          Thread.sleep(10 * 1000L)
        }
      }
    })

  }

}
