package com.tencent.openrc.MSM.CS.join

import java.io.InputStream

import com.tencent.dp.conf.InputConf
import com.tencent.dp.util.{DataUtil, IoUtil, PrintUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by Administrator on 2016/5/11.
 */
object IdMapping {

  val NORMAL_LIMIT = 100

  var tdw_user: String = "tdw_groverli"
  var tdw_passwd: String = "ligrover"

  var configs: Map[String, InputConf] = Map[String, InputConf]()

  def load(config_stream: InputStream): Unit = {
    val config = XML.load(config_stream)
    configs = (config \ "item").map(n => {
      val id = (n \ "id" \ "@name").toString()
      val input_conf = new InputConf
      input_conf.parse((n \ "input").head)
      (id, input_conf)
    }).filter(item => item._1 != null && item._1.nonEmpty && !item._2.isEmpty()).toMap
  }

  def idMapping(sc: SparkSession, data: DataFrame, from_id: String, to_id: String): DataFrame = {
    if (!configs.contains(from_id)) {
      PrintUtil.printLog(s"$from_id is not in id mapping configure!")
      return data
    }
    var cols = new ArrayBuffer[Column]
    var mapping_data: DataFrame = IoUtil.loadData(sc, configs.get(from_id).get, tdw_user, tdw_passwd)
    mapping_data = mapping_data.select(mapping_data(from_id).cast(StringType).as(from_id),
      mapping_data(to_id).cast(StringType).as(to_id))
    for (fn <- data.schema.fieldNames) {
      if(fn != from_id) {
        cols += data(fn)
      }
    }
    cols += mapping_data(to_id).as(to_id)
    var result: DataFrame = data.join(mapping_data, data(from_id) === mapping_data(from_id), "left_outer").select(cols: _*)
    result = result.filter(result(to_id).isNotNull && result(to_id) != "0")
    result
  }

  def idMapping(sc: SparkSession, rdd: RDD[(String, Array[String])],
                from_id: String, to_id: String): RDD[(String, Array[String])] = {
    if (!configs.contains(to_id)) {
      PrintUtil.printLog(s"$to_id is not in id mapping configure!")
      return rdd
    }
    val (input_rdd, input_schema) = IoUtil.loadRdd(sc, configs.get(to_id).get, tdw_user, tdw_passwd)
    val (use_rdd, use_schema) = DataUtil.select(input_rdd, input_schema, Array(from_id, to_id))

    val from_index = use_schema.indexOf(from_id)
    val to_index = use_schema.indexOf(to_id)
    if (from_index < 0 || to_index < 0) {
      PrintUtil.printLog(s"$from_id / $to_id is not in schema!")
      return rdd
    }
    val filter_rdd = DataUtil.filterIllegalId(DataUtil.filterIllegalId(use_rdd, use_schema, from_id),
      use_schema, to_id)
    val output_rdd = rdd.join(filter_rdd.map(item => (item(from_index), item(to_index)))).map(item => {
      (item._2._2, item._2._1)
    })
    output_rdd
  }


  def idMapping(sc: SparkSession, rdd: RDD[Array[String]], schema: Array[String],
                from_id: String, to_id: String): (RDD[Array[String]], Array[String]) = {
    if (!configs.contains(to_id)) {
      PrintUtil.printLog(s"$to_id is not in id mapping configure!")
      return (rdd, schema)
    }
    val id_index = schema.indexOf(from_id)
    if (id_index < 0) {
      PrintUtil.printLog(s"$from_id is not in schema!")
      return (rdd, schema)
    }
    val (input_rdd, input_schema) = IoUtil.loadRdd(sc, configs.get(to_id).get, tdw_user, tdw_passwd)
    val (use_rdd, use_schema) = DataUtil.select(input_rdd, input_schema, Array(from_id, to_id))

    val from_index = use_schema.indexOf(from_id)
    val to_index = use_schema.indexOf(to_id)
    if (from_index < 0 || to_index < 0) {
      PrintUtil.printLog(s"$from_id / $to_id is not in schema!")
      PrintUtil.printLog("schema: " + use_schema.mkString(","))
      return (rdd, schema)
    }

    val filter_rdd = DataUtil.filterIllegalId(DataUtil.filterIllegalId(use_rdd, use_schema, from_id),
      use_schema, to_id)
    val output_rdd = rdd.map(item => (item(id_index), item)).filter(item => DataUtil.isLegalId(item._1)).
      join(filter_rdd.map(item => (item(from_index), item(to_index)))).map(item => {
      val l = item._2._1
      l.update(id_index, item._2._2)
      l
    })
    schema.update(id_index, to_id)
    (output_rdd, schema)
  }

  def cut(rdd: RDD[(String, String)]): RDD[(String, String)] = {
    val distinct_rdd = rdd.distinct()
    val normal_ids = distinct_rdd.map(item => (item._1, 1L)).reduceByKey(_ + _).filter(_._2 < NORMAL_LIMIT.toLong)
    val return_rdd = distinct_rdd.join(normal_ids).map(item => (item._1, item._2._1))
    //    distinct_rdd.unpersist(false)
    return_rdd
  }

  def idMappingFeatureRdd(sc: SparkSession, rdd: RDD[(String, Array[(Int, Array[(String, Float)])])],
                          from_id: String, to_id: String): RDD[(String, Array[(Int, Array[(String, Float)])])] = {
    if (!configs.contains(to_id)) {
      PrintUtil.printLog(s"$to_id is not in id mapping configure!")
      return rdd
    }
    val (input_rdd, input_schema) = IoUtil.loadRdd(sc, configs.get(to_id).get, tdw_user, tdw_passwd)
    val (use_rdd, use_schema) = DataUtil.select(input_rdd, input_schema, Array(from_id, to_id))

    val from_index = use_schema.indexOf(from_id)
    val to_index = use_schema.indexOf(to_id)
    if (from_index < 0 || to_index < 0) {
      PrintUtil.printLog(s"$from_id / $to_id is not in schema!")
      return rdd
    }
    val filter_rdd = DataUtil.filterIllegalId(DataUtil.filterIllegalId(use_rdd, use_schema, from_id),
      use_schema, to_id)
    val output_rdd = rdd.join(cut(filter_rdd.map(item => (item(from_index), item(to_index))))).map(item => {
      (item._2._2, item._2._1)
    })
    output_rdd
  }

  def idMappingFeatureRddGlenn(sc: SparkSession, rdd: RDD[(String, Array[String])],
                               from_id: String, to_id: String): RDD[(String, Array[String])] = {
    if (!configs.contains(from_id)) {
      PrintUtil.printLog(s"$from_id is not in id mapping configure!")
      return rdd
    }

    println(s"idmapping $from_id to $to_id , use table : " + configs.get(from_id).get.db_name + "::" + configs.get(from_id).get.tbl_name)
    val (input_rdd, input_schema) = IoUtil.loadRdd(sc, configs.get(from_id).get, tdw_user, tdw_passwd)
    val (use_rdd, use_schema) = DataUtil.select(input_rdd, input_schema, Array(from_id, to_id))

    val from_index = use_schema.indexOf(from_id)
    val to_index = use_schema.indexOf(to_id)
    if (from_index < 0 || to_index < 0) {
      PrintUtil.printLog(s"$from_id / $to_id is not in schema!")
      return rdd
    }
    val filter_rdd = DataUtil.filterIllegalId(DataUtil.filterIllegalId(use_rdd, use_schema, from_id),
      use_schema, to_id)
    //    val output_rdd = rdd.join(cut(filter_rdd.map(item => (item(from_index), item(to_index))))).map(item => {
    //      (item._2._2, item._2._1)
    val output_rdd = rdd.join(cut(filter_rdd.map(item => (item(to_index), item(from_index))))
      .map(item => (item._2, item._1))).map(item => {
      (item._2._2, item._2._1)
    })
    output_rdd
  }
}


