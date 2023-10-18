/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

import ExecutorState.ExecutorState

import scala.collection.immutable.HashMap
import scala.collection.mutable

 trait Context {

  private val vars: mutable.HashMap[String, String] = mutable.HashMap()

  private var state: ExecutorState = ExecutorState.CREATED

  private var _result: Option[Any] = Option.empty

  def result: Option[Any] = {
    state match {
      case ExecutorState.Compeleted => _result
      case ExecutorState.Terminated => _result
      case _ => throw new Exception("Executor has not finished")
    }
  }

  // scalastyle:off
  def result_=(res: Any): Unit = {
    _result = Option(res)
  }

  def setVars(map : HashMap[String, String]): Unit = {
    vars.clear()
    map.foreach(e => vars.put(e._1, e._2))
  }

  def addVar(key: String, value: String): Unit = {
    vars.put(key, value)
  }

  def getProperty(key: String): Option[String] = {
    vars.get(key)
  }

  def getProperties: Map[String, String] = {
    vars.toMap
  }

  def getPropertyOrThrow(key: String, errorMessage: String = ""): String = {
    val property = vars.get(key)
    if (property.isDefined) {
      property.get
    } else {
      val realMsg = if (errorMessage.isEmpty) {
        s"missing ${key}"
      } else {
        errorMessage
      }
      throw new IllegalArgumentException(realMsg)
    }
  }



  def executorInitialized(): Unit = {
    state = ExecutorState.INIT
    _result = Option.empty
  }

  def executorGetReady(): Unit = {
    state match {
      case ExecutorState.INIT => state = ExecutorState.READY
      case ExecutorState.READY =>
      case _ => throw StateTransitionException(s"expected: ExecutorState.INIT, actual: ${state}")
    }
  }

  def executorWorking(): Unit = {
    state match {
      case ExecutorState.READY => state = ExecutorState.EXECUTING
      case ExecutorState.EXECUTING =>
      case _ => throw StateTransitionException(s"expected: ExecutorState.READY, actual: ${state}")
    }
  }

  def executorTerminated(): Unit = {
    state match {
      case ExecutorState.Compeleted =>
      case ExecutorState.FAILED =>
      case _ => state = ExecutorState.Terminated
    }
  }

  def executorCompleted(): Unit = {
    state match {
      case ExecutorState.EXECUTING => state = ExecutorState.Compeleted
      case ExecutorState.Compeleted =>
      case _ => throw StateTransitionException(s"expected: ExecutorState.EXECUTING, actual: ${state}")
    }
  }

  def executorFailed(): Unit = {
    state = ExecutorState.FAILED
  }

  def isExecutorReady(): Boolean = {
    state != ExecutorState.CREATED && state != ExecutorState.INIT
  }

  def isExecutorCompeleted(): Boolean = {
    state == ExecutorState.Compeleted
  }

  def isExecutorTerminated(): Boolean = {
    state == ExecutorState.Terminated
  }


}
