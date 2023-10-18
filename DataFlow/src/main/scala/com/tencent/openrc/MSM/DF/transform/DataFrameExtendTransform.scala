package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession,Row}

import  org.apache.spark.sql.types.{StringType,IntegerType, LongType, FloatType,DoubleType, StructType,StructField}
class DataFrameExtendTransform (id: String) extends UnaryNode[DataFrame, DataFrame](id) {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val ss = SparkRegistry.spark

  override def doExecute(input: DataFrame): DataFrame = {
    //val outputRootPath = getPropertyOrThrow("outputPath")
    val extend_field_name = getPropertyOrThrow("extend_field")
    val field_value_list = getPropertyOrThrow("field_value_list").split(",")
    val key_field_name=getPropertyOrThrow("key_field")

    val old_field_list=input.schema.map(term=>(term.name,term.dataType)).filter(term=>term._1 != key_field_name && term._1 != extend_field_name)

    val old_field_listbc = SparkRegistry.sc.broadcast(old_field_list)
    val new_field_listbc = SparkRegistry.sc.broadcast(field_value_list)


    val extendDF=input.rdd.map(line=>{
      val key_field=line.getAs[String](key_field_name)
      val extend_field=line.getAs[String](extend_field_name)
      val other_field_array=old_field_listbc.value.map(term=>{
        term._2 match {
          case StringType => line.getAs[String](term._1)
          case IntegerType => line.getAs[Int](term._1).toString
          case LongType => line.getAs[Long](term._1).toString
          case FloatType => line.getAs[Float](term._1).toString
          case DoubleType => line.getAs[Double](term._1).toString
          case _ => line.getAs[String](term._1)
        }
      }).toArray
      (key_field,(extend_field, other_field_array))
    }).groupByKey().map(line=>{
      val key=line._1
      val value_map=line._2.toMap
      val n=line._2.map(_._2.length).max
      val fillArray=new Array[String](n).map(term=>"-1")

      val extended_field=new_field_listbc.value.flatMap(term=>value_map.getOrElse(term, fillArray))
      val row_array=extended_field ++ Array(key)
      Row(row_array:_*)
    })


    val schema= StructType(
      field_value_list.flatMap(term => {
        old_field_list.map(term=>{
          val name=term._1
          StructField(s"${term}_"+s"${name}", StringType)
        })

      }) ++ Array(StructField("wuid", StringType)).toList
    )


    val finalDF=ss.createDataFrame(extendDF, schema)
    finalDF.show(10)
    finalDF
  }
}
