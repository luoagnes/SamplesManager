package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.CS.util.IO.{printlog, rmHdfs}
import com.tencent.openrc.MSM.CS.util.FormatTransform.Text2libSvm
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class Text2libSvmTransform (id: String) extends UnaryNode[RDD[String], RDD[String]](id) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val sc = SparkRegistry.sc

  override def doExecute(input: RDD[String]): RDD[String] = {
    val delimeter = getPropertyOrThrow("delimeter")
    val label_idx = getPropertyOrThrow("label_idx").toInt
    printlog("delimeter: " + delimeter)
    printlog("label_idx: " + label_idx.toString)

    input.take(5).foreach(printlog)
    print("------------------------------------------------------------------------------")
    val data=input.map(line=>{
      val tmp=line.split(delimeter)
      val buffer = ArrayBuffer[String]()
      val n=tmp.length - 1
      for(i<-0 to n){
        if(i>1) buffer +=tmp(i)
      }
      buffer.mkString(delimeter)
    })//))
    data.take(5).foreach(printlog)
    val (resultRDD, feats)=Text2libSvm(sc,data,delimeter,label_idx-2)
    resultRDD.take(10).foreach(printlog)

    val indexpath="hdfs://ss-teg-4-v2/user/datamining/agneswluo/AMS/Experiment/brand/samples/20230625/libsvm_index_map"
    rmHdfs(indexpath)
    feats.map(line=>line._1+":"+line._2.toString).saveAsTextFile(indexpath)
    resultRDD

  }
}
