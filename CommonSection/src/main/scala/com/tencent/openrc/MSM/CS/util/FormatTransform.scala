package com.tencent.openrc.MSM.CS.util
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

object FormatTransform {
  def Text2libSvm(sc:SparkContext, data:RDD[String],delimeter:String, label_index:Int): (RDD[String],RDD[(String,Long)]) = {
    val feats=data.flatMap(line=>{
      val tmp=line.split(delimeter)
      val n=tmp.length-1
      val result = new ArrayBuffer[String]
      for(i <- 0 to n){
        result += i.toString+"_"+tmp(i)
      }
      result.map(term=>(term.split("_")(0).toInt,term.split("_")(1))).filterNot(term=>term._1==label_index).filter(term => term._2 != "0" && term._2 != "0.0" && term._2 != "100000000" && term._2 != "100000000.0")
        .map(term => "c" + term._1.toString + "_" + term._2)
    }).distinct().zipWithIndex().map(line=>(line._1, line._2+1))
    feats.take(10).foreach(println)

    val featsMap=sc.broadcast(feats.collectAsMap())
    val maxindex=featsMap.value.values.max
    println("max index is: "+maxindex.toString)

    val libsvmRDD=data.map(line=>{
      val tmp=line.split(delimeter)
      val label=tmp(label_index)

      val n = tmp.length - 1
      val result = new ArrayBuffer[String]
      for (i <- 0 to n) {
        result += i.toString+"_"+tmp(i)
      }

      val feat_list=result.map(term=>(term.split("_")(0).toInt, term.split("_")(1))).filterNot(term=>term._1==label_index).filter(term => term._2 != "0" && term._2 != "0.0" && term._2 != "100000000" && term._2 != "100000000.0")
        .map(
          term=>{
            val key="c" + term._1.toString + "_" + term._2
            val index=featsMap.value.getOrElse(key, -1L)
            (index,term._2)
          }
        ).filter(_._1 > -1).sortBy(_._1).map(term=>term._1.toString+":"+term._2).mkString(" ")

      label +" "+ feat_list
    })

    (libsvmRDD, feats)
  }
}
