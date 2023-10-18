/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

object ExecutorState extends Enumeration {
  type ExecutorState = Value
  val FAILED, CREATED, INIT, READY, EXECUTING, Terminated, Compeleted = Value
}