package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.CS.util.IO.printlog
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}


class RDDtoDataFrameTransform (id: String) extends UnaryNode[RDD[String], DataFrame](id) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val sc = SparkRegistry.sc

  override def doExecute(input: RDD[String]): DataFrame = {

    val fieldListstr = getPropertyOrThrow("fieldList")
    val typeListstr = getPropertyOrThrow("typeList")

    printlog("fieldList: " + fieldListstr)
    printlog("typeList: " + typeListstr)

    val fieldList = fieldListstr.split(",")
    val typeList= typeListstr.split(",")
    val n=fieldList.length

    val schema=StructType((0 to n-1).toArray.map(i=>{
        val fieldname = fieldList(i)
        val fieldtype = typeList(i) match {
          case "str" => StringType
          case "int" => IntegerType
          case "float" => FloatType
          case "double" => DoubleType
          case _ => null
        }
        StructField(fieldname, fieldtype)
      }).toList
    )

    val transRDD=input.map(line=>{
      val tmp=line.split(",")
      val n=tmp.length
      val data=(0 to n-1).toArray.map(i=>{
        typeList(i) match {
          case "int" => tmp(i).trim.toInt
          case "float" => tmp(i).trim.toFloat
          case "double" => tmp(i).trim.toDouble
          case _ => tmp(i).trim
        }
      })
      data
    }).map{x=>Row(x: _*)}
    printlog("Rdd to dataframe success !!!")

    SparkRegistry.spark.createDataFrame(transRDD, schema)
  }
}
