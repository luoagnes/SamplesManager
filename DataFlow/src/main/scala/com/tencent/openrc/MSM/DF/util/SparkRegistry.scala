package com.tencent.openrc.MSM.DF.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkRegistry {

  lazy val conf: SparkConf = new SparkConf()
    .setAppName("RocksFlow Application")
  lazy val spark: SparkSession = {
    val use_kyro: Boolean =
      conf.get("spark._.nabu.use_kyro", "true").toBoolean
    if (use_kyro) {
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }
    //    conf.set("spark.master", "local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }
  lazy val sc: SparkContext = spark.sparkContext

  lazy val duration = 60
  lazy val ssc =  new StreamingContext(sc, Seconds(duration))
}
