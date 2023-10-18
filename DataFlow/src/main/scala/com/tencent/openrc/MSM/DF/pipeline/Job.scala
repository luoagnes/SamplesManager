/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

import scala.beans.BeanProperty
import scala.collection.immutable.HashMap
import scala.collection.mutable

class Job {

  @BeanProperty
  var id: Long = _

  @BeanProperty
  var configString: String = _

  @BeanProperty
  var pipeline: Pipeline = _

  @BeanProperty
  var name: String = _

  @BeanProperty
  var authors: String = _

  private val configs: mutable.HashMap[String, String] = mutable.HashMap()

  def setConfigs(map : HashMap[String, String]): Unit = {
    configs.clear()
    map.foreach(e => configs.put(e._1, e._2))
  }

  def addConfig(key: String, value: String): Unit = {
    configs.put(key, value)
  }

  def getConfig(key: String): Option[String] = {
    configs.get(key)
  }

  def getConfigOrElse(key: String, defaultValue: String): String = {
    configs.getOrElse(key, defaultValue)
  }
}
