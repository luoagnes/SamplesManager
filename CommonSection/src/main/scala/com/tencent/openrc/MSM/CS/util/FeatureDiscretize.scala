package com.tencent.openrc.MSM.CS.util

import com.tencent.openrc.MSM.CS.common.Constant
import com.tencent.openrc.MSM.CS.conf.FeatureConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import com.tencent.openrc.MSM.CS.conf.DataConf


import scala.collection.mutable.ArrayBuffer

/**
 * Created by groverli on 2016/5/10.
 */
object FeatureDiscretize {

  def binarySearch(boundarys: Array[Double], x: Double): Int = {
    var left: Int = 0
    var right: Int = boundarys.length - 1
    if (x > boundarys(right))
      return boundarys.length
    while (left < right) {
      val mid : Int = (left + right) / 2
      if (x > boundarys(mid))
        left = mid + 1
      else
        right = mid
    }
    left
  }

  def binarySearch(boundarys: Array[Float], x: Float): Int = {
    var left: Int = 0
    var right: Int = boundarys.length - 1
    if (x > boundarys(right))
      return boundarys.length
    while (left < right) {
      val mid : Int = (left + right) / 2
      if (x > boundarys(mid))
        left = mid + 1
      else
        right = mid
    }
    left
  }

  def linearSearch(boundarys: Array[Double], x: Double): Int = {
    var i = 0
    while (i < boundarys.length) {
      if (x <= boundarys(i))
        return i
      else
        i += 1
    }
    i
  }

  def linearSearch(boundarys: Array[Float], x: Float): Int = {
    var i = 0
    while (i < boundarys.length) {
      if (x <= boundarys(i))
        return i
      else
        i += 1
    }
    i
  }

  def linearSearch(boundarys: Array[Float], x: Long): Int = {
    var i = 0
    while (i < boundarys.length) {
      if (x <= boundarys(i))
        return i
      else
        i += 1
    }
    i
  }

  def linearSearch(boundarys: Array[Float], x: Int): Int = {
    var i = 0
    while (i < boundarys.length) {
      if (x <= boundarys(i))
        return i
      else
        i += 1
    }
    i
  }

  def linearSearch(boundarys: Array[Float], x: Double): Int = {
    var i = 0
    while (i < boundarys.length) {
      if (x <= boundarys(i))
        return i
      else
        i += 1
    }
    i
  }

  def equiFreqDiscretization(distribution: Array[(Double, Long)], num: Int): Array[Double] = {
    val sum = distribution.map(_._2).sum
    val width: Long = sum / num
    val sorted_distribution: Array[(Double, Long)] = distribution.sortBy(_._1)
    var current_num = 0L
    val boundarys = new ArrayBuffer[Double]
    for (i <- sorted_distribution.indices) {
      current_num += sorted_distribution(i)._2
      if (boundarys.length < (num - 1)) {
        if (current_num >= ((boundarys.length + 1) * width)) {
          boundarys += sorted_distribution(i)._1
        }
      }
    }
    boundarys.toArray
  }

  def equiFreqDiscretization(distribution: Array[(Float, Long)], num: Int): Array[Float] = {
    val sum = distribution.map(_._2).sum
    val width: Long = sum / num
    val sorted_distribution = distribution.sortBy(_._1)
    var current_num = 0L
    val boundarys = new ArrayBuffer[Float]
    for (i <- sorted_distribution.indices) {
      current_num += sorted_distribution(i)._2
      if (boundarys.length < (num - 1)) {
        if (current_num >= ((boundarys.length + 1) * width)) {
          boundarys += sorted_distribution(i)._1
        }
      }
    }
    boundarys.toArray
  }

  def equiValDiscretization(distribution: Array[(Double, Long)], num: Int): Array[Double] = {
    val max = distribution.map(_._1).max
    val min = distribution.map(_._1).min
    val width = (max - min) / num
    val boundarys = new ArrayBuffer[Double]
    for(i <- 1 until num)
      boundarys += (min + i * width)
    boundarys.toArray
  }

  def equiValDiscretization(distribution: Array[(Float, Long)], num: Int): Array[Float] = {
    val max = distribution.map(_._1).max
    val min = distribution.map(_._1).min
    val width = (max - min) / num
    val boundarys = new ArrayBuffer[Float]
    for(i <- 1 until num)
      boundarys += (min + i * width)
    boundarys.toArray
  }


  def discretization(distribution: Array[(Double, Long)], feature_conf: FeatureConf): Array[Double] = {
    if (!feature_conf.discretize)
      return null
    if (distribution == null)
      return null
    feature_conf.discretize_type match {
      case Constant.DISCRETIZE_EQUI_FREQ => equiFreqDiscretization(distribution, feature_conf.discretize_argv)
      case Constant.DISCRETIZE_EQUI_VAL => equiValDiscretization(distribution, feature_conf.discretize_argv)
      case _ => null
    }
  }



  def discretization(distribution: Array[(Float, Long)], feature_conf: FeatureConf): Array[Float] = {
    if (!feature_conf.discretize)
      return null
    if (distribution == null)
      return null
    feature_conf.discretize_type match {
      case Constant.DISCRETIZE_EQUI_FREQ => equiFreqDiscretization(distribution, feature_conf.discretize_argv)
      case Constant.DISCRETIZE_EQUI_VAL => equiValDiscretization(distribution, feature_conf.discretize_argv)
      case _ => null
    }
  }


  def discretization(distribution: Array[(Float, Long)], discretize_type:String, num:Int): Array[Float] = {
    discretize_type match {
      case Constant.DISCRETIZE_EQUI_FREQ => equiFreqDiscretization(distribution, num)
      case Constant.DISCRETIZE_EQUI_VAL => equiValDiscretization(distribution, num)
      case _ => null
    }
  }

  def discretizeTable(feature_rdd: RDD[(String, Array[String])],
                      feature_schema: Array[String],
                      bounds: Map[Int, Array[Float]],
                      data_conf: DataConf): RDD[(String, Array[String])] = {

    val discretize_list = data_conf.feature_conf_list.filter(feature_conf => feature_conf.discretize).map(feature_conf => {
      val index = feature_schema.indexOf(feature_conf.value_field)
      val bound = bounds.getOrElse(feature_conf.feature_index, Array[Float](0))
      (index, bound)
    }).filter(_._1 >= 0).filter(_._2.nonEmpty).toMap

    if (discretize_list.isEmpty)
      feature_rdd
    else
      feature_rdd.map(item => {
        val features =
          item._2.zipWithIndex.map(term => {
            val bounds = discretize_list.get(term._2)
            if (bounds.isDefined) {
              try {
                val v = term._1.toFloat
                if (v < 0.0F)
                  null
                else
                  FeatureDiscretize.linearSearch(bounds.get, v).toString
              }
              catch {
                case _: Throwable => null
              }
            }
            else {
              term._1
            }
          })
        (item._1, features)
      })
  }


  def discretizeTable(sc: SparkContext,
                      rdd: RDD[Array[String]],
                      schema: Array[String],
                      features: Array[FeatureConf]): (RDD[Array[String]], Array[(String, Array[Float])]) = {

    val discretize_features = features.filter(_.discretize).map(feature_conf =>
      (schema.indexOf(feature_conf.value_field), feature_conf)).filter(_._1 >= 0)

    if (discretize_features.isEmpty)
      return (rdd, new Array[(String, Array[Float])](0))

    val discretize_index_bc = sc.broadcast(discretize_features.map(_._1))

    rdd.persist(StorageLevel.DISK_ONLY)
    val distribution = rdd.flatMap(item => {
      discretize_index_bc.value.map(index => {
        val value =
          try {
            item(index).toFloat
          }
          catch {
            case _: Throwable => -1.0F
          }
        (index, value)
      }).filter(_._2 > 0)
    }).map(item => (item, 1L)).reduceByKey(_ + _).collect().groupBy(_._1._1).
      mapValues(_.map(item => (item._1._2, item._2)))

    val bounds = distribution.map(item => {
      (item._1, item._2, discretize_features.toMap.get(item._1))
    }).filter(_._3.isDefined).map(item => {
      (item._1, discretization(item._2, item._3.get))
    }).toMap

    val bounds_bc = sc.broadcast(bounds)

    val output_rdd = rdd.map(item => {
      item.zipWithIndex.map(term => {
        if (bounds_bc.value.contains(term._2)) {
          try {
            linearSearch(bounds_bc.value.get(term._2).get, term._1.toFloat).toString
          }
          catch {
            case _: Throwable => term._1
          }
        }
        else {
          term._1
        }
      })
    })
    rdd.unpersist(false)
    (output_rdd, bounds.toArray.map(item => (schema(item._1), item._2)))
  }

  def discretizeByBoundary(rdd: RDD[Row],
                           id_name: String,
                           value_name: String,
                           boundarys: Array[Double]): RDD[Row] = {
    rdd.map(row => {
      val x = row.getAs[Double](value_name)
      Row(row.getAs[String](id_name), (boundarys, x))
    })
  }

}


