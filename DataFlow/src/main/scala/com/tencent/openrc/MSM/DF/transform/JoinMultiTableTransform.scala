package com.tencent.openrc.MSM.DF.transform
import com.tencent.openrc.MSM.CS.operation.Datahandler.{FeatureCollect, loadActionDF}
import com.tencent.openrc.MSM.CS.operation.Readparas.readWholeConfigure
import com.tencent.openrc.MSM.CS.util.IO.{printlog}
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import java.util.Calendar
import scala.util.Random
import scala.collection.JavaConversions._
import com.tencent.openrc.MSM.CS.util.IO.{printlog, rmHdfs, saveAsText}

import java.util
import scala.collection.immutable.HashSet
import scala.collection.mutable

class JoinMultiTableTransform (id: String) extends UnaryNode[Unit, DataFrame](id) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val ss = SparkRegistry.spark
  //Iterator.continually()

  override def doExecute(input: Unit): DataFrame = {
    val currDate = getPropertyOrThrow("currDate")
//    val date=Calendar.getInstance()
////    val currYear=date.get(Calendar.YEAR)
////    val currMonth=date.get(Calendar.MONTH)
////    val currDay=date.get(Calendar.DAY_OF_MONTH)
//    val currDate=java.time.LocalDate.now.toString.replaceAll("-","")
//    //currYear.toString+currMonth.toString+currDay.toString
    val configure_path =  getPropertyOrThrow("configure_path")

    printlog("currDate: " + currDate)
    printlog("configure_path: " + configure_path)

    val configure = readWholeConfigure(ss, configure_path) // 读取整个configure
    printlog("feature config load over!")

    val actionDF = loadActionDF(ss, configure, currDate) // 读取actionrdd
    val joinDF = FeatureCollect(ss, configure, actionDF, currDate)
    joinDF.show()
    joinDF

  }
}


object JoinMultiTableTransform{
  def main(args: Array[String]): Unit = {
    val ones: Iterator[Int] = Iterator.continually(1)
    print(ones.take(10))


  }
}
