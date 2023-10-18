package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.slf4j.LoggerFactory
import com.tencent.openrc.MSM.CS.util.IO.{printlog}
import com.tencent.openrc.MSM.CS.operation.Datahandler.{discretizeData, getDiscretizeBounds}
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


class DiscretizeTransfrom (id: String) extends UnaryNode[DataFrame, DataFrame](id) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val sc = SparkRegistry.sc
  private val ss = SparkRegistry.spark

  override def doExecute(input: DataFrame): DataFrame= {
    val (jobName, jobType) = ("", "")

    val isBoundaryExists0=getPropertyOrThrow("is_boundary_exists")
    val discretizeFieldsIdxList = getPropertyOrThrow("discretize_fields_idx").split(",")
    val discretizeFieldsNameList = getPropertyOrThrow("discretize_fields_name").split(",")
    val discretizeMethod = getPropertyOrThrow("method")
    val discretizeRootPath=getPropertyOrThrow("discretize_path")
    val boundary_num = getProperty("boundary_num").getOrElse("10").toInt
    val drop_fields_name= getPropertyOrThrow("drop_fields_name")
    val drop_fields_list=drop_fields_name.toString.split(",")
    printlog("boundary_num: " + boundary_num.toString)

    printlog("isBoundaryExists: " + isBoundaryExists0)
    printlog("discretizeFieldsIdxList: " + discretizeFieldsIdxList)
    printlog("discretizeFieldsNameList: " + discretizeFieldsNameList)
    printlog("discretizeMethod: " + discretizeMethod)
    printlog("discretizeRootPath: " + discretizeRootPath)
    printlog("drop_fields_name: " + drop_fields_name)

    val BoundaryExistsList=isBoundaryExists0.split(",").map(_.toInt)
    val isBoundaryExists=BoundaryExistsList.filter(e=>e>0).length > 0
    val n=discretizeFieldsIdxList.length - 1
    //input.take(5).foreach(printlog)

    var inputDF=input
    printlog("--------------- before drop -------------")
    printlog(inputDF.schema.map(_.name).mkString(","))
    input.show()
    printlog("--------------- before after -------------")

    for(fea <- drop_fields_list){
      inputDF=inputDF.drop(fea)
    }

    val remain_feature_list=inputDF.schema.map(_.name)
    for(fea <- remain_feature_list){
      inputDF=inputDF.withColumn(fea,when(col(fea).isNull ,0L).otherwise(col(fea)))
    }

    printlog(inputDF.schema.map(_.name).mkString(","))
    inputDF.show()

    val discretize_features = (0 to n).filter(i => BoundaryExistsList(i) == 0).map(i => (discretizeFieldsNameList(i), discretizeFieldsIdxList(i).toInt)).toArray
    printlog("need discretize feature list: " + discretize_features.map(_._1).mkString(","))

    val final_discretize_features_list=discretize_features.map(_._1)
    var boundarys=getDiscretizeBounds(final_discretize_features_list,boundary_num, inputDF, discretizeMethod)
    printlog("boundarys num: "+boundarys.length.toString)

    val discretizePath=discretizeRootPath+"/" + discretizeMethod
    if(isBoundaryExists){
      printlog("some discretize boundarys exists !")
      val oldboundarys= sc.textFile(discretizePath).filter(line => line.split(",").length > 1).map(line => {
        val tmp = line.split(",")
        val key = tmp(0)
        val boundary=tmp(1).split(";").map(term=>term.toFloat)
        (key,boundary)

      }).collect()
     // val adddiscretize=boundarys.map(line=>line._1+","+line._2.map(_.toString).mkString(";")).map(line=>)
//        saveAsText(adddiscretize,discretizePath)
      boundarys = boundarys ++ oldboundarys
        printlog("exists discretize boundarys list: "+boundarys.map(_._1).mkString(","))
    }
    printlog("get boundarys over !!!")

    //rmHdfs(discretizePath)
    //sc.parallelize(boundarys.map(line=>line._1+","+line._2.map(_.toString).mkString(";"))).saveAsTextFile(discretizePath)

    val feat2Idx=(0 to n).map(i=>(discretizeFieldsNameList(i), discretizeFieldsIdxList(i).toInt)).toMap
    val boundaryArray=boundarys.map(item=>(item._1, item._2)).toMap
    printlog("discretize feature list: "+boundaryArray.keys.mkString(","))
    printlog("get boundary over !!!")
//    val boundaryArraybc=sc.broadcast(boundaryArray)
    // 离散化
    //println(input.schema.map(_.name).mkString(","))

    val input2=inputDF.withColumn("label",when(col("conv_num") >= 110,1L).otherwise(0L))
    val schema=StructType(input2.schema.map(term=>StructField(term.name,LongType, true)).toList)


    //println("---------after add label ---------------")
    //println(input2.schema.map(_.name).mkString(","))
    val discretizeRDD0=discretizeData(sc, input2, boundaryArray)
    print("----------------print discretize result ------------")
    discretizeRDD0.take(10).foreach(printlog)
    print("----------------print discretize result ------------")
    val discretizeRDD=discretizeRDD0.map(line=>{
      val tmp=line.split(",")
      val rowdata=tmp.map(term=> term.toLong)
      Row(rowdata:_*)
    })
    printlog("-----discretizeRDD lines: "+discretizeRDD.count().toString)
    printlog("discretize over !!!")
    val discretizeDF=ss.createDataFrame(discretizeRDD, schema)
    discretizeDF.show()
    discretizeDF
  }
}
