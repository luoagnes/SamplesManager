/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

/*
 * 可执行trait
 */
trait Executable {

  var startTime: Long = 0

  var endTime: Long = 0

  def init(): Boolean = true

  def isReady: Boolean = true

  def isCompleted: Boolean = false

  def isTerminated: Boolean = false

  def preExecute(): Unit = {
    startTime = System.currentTimeMillis()
  }

  def execute(): Unit

  def fail(): Unit = {
    endTime = System.currentTimeMillis()
  }

  def postExecute(): Unit = {
    endTime = System.currentTimeMillis()
  }

}
