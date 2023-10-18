package com.tencent.openrc.MSM.CS.util
import com.tencent.openrc.MSM.CS.util.IO.printlog
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD

/**
 * Created by agneswluo on 2023/5/10.
 */
class SampleOperator {

  /**
   * sampling base on field value layer2
   *
   * @param fractions : org.apache.spark.sql.SparkSession
   * @param dataDF    : configure object
   * @param keyField  :
   */
  def SampleByGroup(fractions: Map[String, Double], dataDF: DataFrame, keyField: String): DataFrame = {
    dataDF.stat.sampleBy(keyField, fractions, 36L)
    dataDF
  }


  /**
   * sampling base on the certain num
   *
   * @param sampleNum     : org.apache.spark.sql.SparkSession
   * @param dataDF : configure object
   * @param keyField:
   */
  def SampleByNum(sampleNum:Int, dataDF:DataFrame, keyField: String): DataFrame = {
    val fractions=dataDF.groupBy(keyField).count.collect().map(r=>{
      val key=r.get(0).toString
      val num=r.get(1).toString.toInt
      val value=if(sampleNum > num) 1.0 else sampleNum / num
      (key, value)
    }).toMap

    val dataDF2=dataDF.withColumn(keyField,col(keyField).cast(StringType))
    SampleByGroup(fractions, dataDF2, keyField)
  }


  /**
   * sampling base on the certain field value, the type of vlaue is int
   *
   * @param FieldValue : org.apache.spark.sql.SparkSession
   * @param dataDF    : configure object
   * @param keyField  :
   * @param times
   */
  def SampleByField(FieldValue: Int, dataDF: DataFrame, keyField: String, times:Int): DataFrame = {
    val groupDF=dataDF.groupBy(keyField).count
    groupDF.show()
    //val sampleNum = groupDF.collect.filter(r=>r.get(0).toString == FieldValue).map(r=>r.get(1).toString.toInt).take(1)(0) * times
    val sampleArray = groupDF.where(keyField + " = " + FieldValue).take(1).map(r => r.get(1).toString.toInt)
    //.take(1)(0) * times
    val sampleNum = sampleArray(0) * times
    printlog("the target label num: "+sampleNum.toString)
    SampleByNum(sampleNum, dataDF, keyField)
  }

  /**
   * sampling base on the certain field value, the type of vlaue is string
   *
   * @param FieldValue : org.apache.spark.sql.SparkSession
   * @param dataDF     : configure object
   * @param keyField   :
   * @param times
   */
  def SampleByField(FieldValue: String, dataDF: DataFrame, keyField: String, times:Int): DataFrame = {
    val groupDF = dataDF.groupBy(keyField).count
    groupDF.show()
    val sampleArray = groupDF.where(keyField+" = "+FieldValue).rdd.map(r => r.getAs[Long](1)).collect()

      //.take(1)(0) * times
    val sampleNum=sampleArray(0).toInt * times
    printlog("the target label num: "+sampleNum.toString)
    SampleByNum(sampleNum, dataDF, keyField)
  }

  def sampleSplit(split_rate_array:Array[Double], iniDF:DataFrame): Array[DataFrame]= {
    val splitDFArray=iniDF.randomSplit(split_rate_array)
    splitDFArray.map(e=>e.toDF)
  }
}
