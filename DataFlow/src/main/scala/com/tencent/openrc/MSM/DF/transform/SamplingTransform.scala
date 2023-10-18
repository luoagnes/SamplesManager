package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.CS.util.IO.{printlog, rmHdfs, saveAsText, saveTFRecord}
import com.tencent.openrc.MSM.CS.util.SampleOperator
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.sql.functions.{col, when}
import java.util.Calendar

class SamplingTransform (id: String) extends UnaryNode[DataFrame, Unit](id) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val sc = SparkRegistry.sc

  override def doExecute(input: DataFrame): Unit = {
    val (jobName, jobType) = ("", "")

    val handleType = getPropertyOrThrow("handle_type")
    val labelField = getPropertyOrThrow("label_field")
    val labelValue = getPropertyOrThrow("label_value")
    val splitStr = getPropertyOrThrow("split")
    val dirNameStr = getPropertyOrThrow("dirName")
    val dateTime = getPropertyOrThrow("dateTime")
//    val date = Calendar.getInstance()
////    val currYear = date.get(Calendar.YEAR)
////    val currMonth = date.get(Calendar.MONTH)
////    val currDay = date.get(Calendar.DAY_OF_MONTH)
//    val dateTime =java.time.LocalDate.now.toString.replaceAll("-","") // currYear.toString + currMonth.toString + currDay.toString

    val outputRootPath = getPropertyOrThrow("outputPath")
    val saveType=getPropertyOrThrow("saveType")

    printlog("handleType: " + handleType)
    printlog("label field name: " + labelField)
    printlog("label value: " + labelValue)
    printlog("split rate distribution: " + splitStr)
    printlog("dirName distribution: " + dirNameStr)
    printlog("output root path: " + outputRootPath)

    input.show()
    val inputDF=input.withColumn("label",when(col("conv_num") >= 110,1).otherwise(0))

    val splitRateArray=splitStr.split(",").map(e=>e.toDouble)
    val dirNameArray=dirNameStr.split(",").map(e=>e.trim)

    val imbalance_rate_str = getPropertyOrThrow("imbalance_rate")
    printlog("imbalance_rate: " + imbalance_rate_str)

    val imbalance_times=imbalance_rate_str.toInt
    val samplesobj=new SampleOperator()

    print("----------print input over ------------------")
    val samplesDF = samplesobj.SampleByField(labelValue, inputDF, labelField, imbalance_times)

    samplesDF.show()

    printlog("saveType is:"+saveType)
    val splitDataArray=samplesobj.sampleSplit(splitRateArray, samplesDF)
    printlog("splitDataArray: "+splitDataArray.length.toString)
    val n=splitDataArray.length - 1
    for(i <- 0 to n){
       val saveData=splitDataArray(i)
       val path=outputRootPath+"/"+dateTime+"/"+dirNameArray(i)
      printlog("start save path: "+path)

      val TextPath = path + "/Text"
      rmHdfs(TextPath)
      saveAsText(saveData, TextPath)
      printlog("text samples data success !!!")

      val schema=samplesDF.schema
      val TFRecordPath=path+"/TFRecord"
      rmHdfs(TFRecordPath)
      saveTFRecord(SparkRegistry.spark, TFRecordPath, saveData)
      printlog("tf samples data success !!!")
    }
    }
}
