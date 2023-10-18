package com.tencent.openrc.MSM.CS.util

import org.apache.spark.rdd.RDD

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.tencent.dp.util.{HdfsUtil, PrintUtil,IoUtil}

object IO {
  def getConfigureFileInputStream(ss: SparkSession, path: String): InputStream = {
    if (path.startsWith("hdfs")) {
      val conf_str = ss.sparkContext.textFile(path).collect()
      val conf_stream = new ByteArrayInputStream(conf_str.mkString(" ").getBytes("utf-8"))
      conf_stream
    }
    else {
      new FileInputStream(path)
    }
  }

  def filterPartition(filterTimeRangeArray: Array[(String, String)], partition: String): Boolean = {
    if (filterTimeRangeArray.length == 0) {
      true
    } else {
      filterTimeRangeArray
        .map(tuple2 => {
          val lower = tuple2._1
          val upper = tuple2._2
          partition < lower || partition > upper
        })
        .reduce(_ && _)
    }
  }

  def loadRdd(): Unit = {
    //IoUtil.loadRdd(spark, configure.action_input_conf, configure.tdw_user, configure.tdw_passwd)
  }


  def saveTFRecord(ss:SparkSession, path: String, data: RDD[Row], schema: StructType): Unit = {
    ss.createDataFrame(data, schema)
      .write
      .format("tfrecords")
      .option("recordType", "Example")
      .mode(SaveMode.Ignore)
      .save(path)

    println("load tfrecords test, path: " + path)
    ss.read.format("tfrecords").schema(schema).load(path).show(10, false)
  }

  def saveTFRecord(ss: SparkSession, path: String, data: DataFrame): Unit = {
    val schema=data.schema
    data.write
      .format("tfrecords")
      .option("recordType", "Example")
      .mode(SaveMode.Ignore)
      .save(path)

    println("load tfrecords test, path: " + path)
    ss.read.format("tfrecords").schema(schema).load(path).show(10, false)
  }

  def saveAsText(input: DataFrame, path: String): Unit = {
    input.write
      .format("csv")
      .mode(SaveMode.Append)
      .save(path)
  }

  def printlog(content:String): Unit = {
    PrintUtil.printLog(content)
  }

  def colorful_println(content: String): Unit = {
    PrintUtil.colorful_println(content)
  }

  def isExistsHdfs(targetPath:String): Boolean = {
    HdfsUtil.isExistHdfsFile(targetPath)

  }

  def rmHdfs(targetPath:String): Unit = {
    HdfsUtil.rmHdfsFile(targetPath)
  }




}

