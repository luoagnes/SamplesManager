package com.tencent.openrc.MSM.CS.driver

import com.tencent.dp.util.{HdfsUtil, PrintUtil}
import com.tencent.openrc.MSM.CS.util.FeatureDiscretize.{discretization, linearSearch}
import org.apache.spark.{SparkConf, SparkContext}

object FearureHandleDriver {

  def main(args:Array[String]): Unit ={
    val source_hdfs=args(0)
    val target_hdfs=args(1)
    val discretize_field_list_str=args(2)
    val leave_field_list_str=args(3)
    val field_num=args(4).toInt
    val boundary_num=args(5).toInt
    val label_index=args(6).toInt
    val data_name=args(7).trim

    PrintUtil.printLog("source_hdfs: "+source_hdfs)
    PrintUtil.printLog("target_hdfs: "+target_hdfs)
    PrintUtil.printLog("discretize_field_list_str: "+discretize_field_list_str)
    PrintUtil.printLog("leave_field_list_str: "+leave_field_list_str)
    PrintUtil.printLog("field_num: "+field_num)
    PrintUtil.printLog("boundary_num: "+boundary_num)
    PrintUtil.printLog("label_index: "+label_index)

    val root_hdfs="hdfs://ss-teg-4-v2/user/datamining/agneswluo/AMS/baopin/"+data_name+"/"

    val conf=new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc=new SparkContext(conf)

    val source_data=source_hdfs.split(";").map(date_dir=>{
      val itemRdd=sc.textFile(root_hdfs+date_dir.trim).filter(line=>line.split(",").length >= field_num).distinct()
      itemRdd
    }).reduce(_.union(_)).distinct()
    PrintUtil.printLog("source_data lines: "+source_data.count().toString)

    val leave_field_list_bc=sc.broadcast(leave_field_list_str.split(",").map(_.toInt))

    val discretize_field_list=discretize_field_list_str.split(",").map(_.toInt)

    val boundarys_list=discretize_field_list.map(index=>{
      val sourceArray=source_data.map(line=>(line.split(",")(index),1)).reduceByKey((x,y)=>x+y).map(line=>{
        var key= if(line._1.trim =="") 0 else line._1.toFloat
          (key, line._2.toLong)
      }).collect().toArray
      val n=sourceArray.map(line=>(line._1,1)).map(_._2).sum

      if(n < boundary_num){
        val boundary = discretization(sourceArray, "equi_freq", 4)
        (index, boundary)
      }else{
        val boundary = discretization(sourceArray, "equi_freq", boundary_num)
        (index, boundary)
      }

    })
    val boundarys_list_bc=sc.broadcast(boundarys_list)

    val discretizeRdd=source_data.map(line=>{
      val tmp=line.split(",")
      val discretize_data=boundarys_list_bc.value.map(item=>{
        val index=item._1
        val boundary=item._2
        val v=if(tmp(index).trim =="") 0 else tmp(index).toFloat
        linearSearch(boundary, v)
      }).map(_.toString)

      val label=tmp(label_index).toString

      val leave_data=leave_field_list_bc.value.map(index=>tmp(index))
      Array(label) ++ leave_data ++ discretize_data
    }).map(_.mkString(",")).distinct()

    if (HdfsUtil.isExistHdfsFile(target_hdfs)){
      HdfsUtil.rmHdfsFile(target_hdfs)
    }
    discretizeRdd.saveAsTextFile(target_hdfs)
  }
}


