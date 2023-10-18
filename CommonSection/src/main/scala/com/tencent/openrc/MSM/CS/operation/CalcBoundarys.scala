package com.tencent.openrc.MSM.CS.operation

object CalcBoundarys {


//  def main(args: Array[String]): Unit = {
//    if (args.length < 1) {
//      System.err.println("Usage: $SPARK_HOME/run com.tencent.dp.da.driver.Driver " + "<feature conf path> " + "[<time>]")
//      System.exit(1)
//    }
//    val currDate = args(0)
//    val configure_path = args(1)
//
//    val ss = SparkSession
//      .builder()
//      .config("spark.3", "true")
//      .config("spark.speculation.interval", "30000")
//      .config("spark.speculation.quantile", "0.75")
//      .config("spark.speculation.multiplier", "1.5")
//      .config("spark.akka.frameSize", "1024")
//      .config("spark.network.timeout", "600000")
//      .config("spark.yarn.am.waitTime", "600000")
//      .config("spark.akka.ask.timeout", "600000")
//      .appName(this.getClass.getSimpleName)
//      .getOrCreate()
//
//    val configure = readWholeConfigure(ss, configure_path) // 读取整个configure
//    PrintUtil.colorful_println("feature config load over!")
//
//    val actionDF = loadActionDF(ss, configure, currDate) // 读取actionrdd
//    actionDF.show()
//
//    val joinDF = FeatureCollect(ss, configure, actionDF, currDate)
//    joinDF.show()
//
//    SampleSave(ss, configure, joinDF)
//    PrintUtil.colorful_println("save data over !!!")
//
//  }

}
