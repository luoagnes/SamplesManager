package com.tencent.openrc.MSM.DF.transform

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import com.tencent.openrc.MSM.CS.common.Constant
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import com.tencent.openrc.MSM.CS.util.IO.printlog
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode

import org.apache.spark.sql.{DataFrame, SparkSession,Row}

import  org.apache.spark.sql.types.{StringType,IntegerType, LongType, FloatType,DoubleType, StructType,StructField}



import java.text.SimpleDateFormat

class HdfsLoader (id: String) extends UnaryNode[Unit, DataFrame](id) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val sc = SparkRegistry.sc

  override def doExecute(input: Unit): DataFrame = {
    val (jobName, jobType) = ("", "")

    val rootPath=getPropertyOrThrow("rootPath")
    val inputPaths = getPropertyOrThrow("inputPath")
    val path_type = getPropertyOrThrow("path_type")
    val field_list=getPropertyOrThrow("field_list")
    val data_type=getPropertyOrThrow("data_type")

    printlog("rootPath: "+rootPath)
    printlog("inputPaths: "+inputPaths)
    printlog("inputPaths: "+inputPaths)
    printlog("field_list: "+field_list)
    printlog("data_type: "+data_type)

    var pathArray=Array[String]()
    if(path_type == "part") {
      pathArray = inputPaths.split(",").flatMap(e => {
        val tmp = e.split("-")
        printlog("tmp lines: " + tmp.length.toString)
        val dateArray = if (tmp.length > 1) {
          val start = tmp(0).toInt
          val end = tmp(1).toInt
          printlog("start: " + start.toString)
          printlog("end: " + end.toString)
          val dateList = (start to end).filter(day => {
            try {
              val formatter = new SimpleDateFormat("yyyyMMdd")
              formatter.setLenient(false)
              formatter.parse(day + "")
              true
            }
            catch {
              case _: Exception => false
            }
          }).map(_.toString).toArray
          printlog(dateList.mkString("@@"))
          dateList
        } else Array(e)

        dateArray
      })
    }else{
      pathArray = inputPaths.split(",")
    }

    val schema = StructType(
      field_list.split(";").map(term => {
        StructField(term, StringType)
      }).toList
    )

    data_type match {
      case "tfrecord" => {
        var iniRDD= SparkRegistry.spark.createDataFrame(sc.emptyRDD[Row], schema)
        for (dirname <- pathArray) {
          val hdfs_path = rootPath + "/" + dirname
          //try {
            val tmpRDD = SparkRegistry.spark.read.format("tfrecord").option("recordType", "Example").load(hdfs_path)
            tmpRDD.show(5)
            iniRDD = iniRDD.union(tmpRDD)
          //} catch {
           // case _: Exception => printlog(hdfs_path + " is not exists !!!")
          //}
        }
        iniRDD
      }
      case _ => {
        var iniRDD = sc.emptyRDD[String]
        for (dirname <- pathArray) {
          val hdfs_path = rootPath + "/" + dirname
          try {
            val tmpRDD = sc.textFile(hdfs_path)
            iniRDD = iniRDD.union(tmpRDD)
          } catch {
            case _: Exception => printlog(hdfs_path + " is not exists !!!")
          }
        }
        val transRDD = iniRDD.map(line => Row(line.split(","): _*))
        val finalDF=SparkRegistry.spark.createDataFrame(transRDD, schema)
        finalDF.show(5)
        finalDF
      }
      }
    }
}
