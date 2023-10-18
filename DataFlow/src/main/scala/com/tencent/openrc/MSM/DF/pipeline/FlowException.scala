/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

case class ExecutorException(message: String) extends Exception(message)

case class ExecutorNotReadyException(message: String) extends Exception(message)

case class ExecutorNotFinishedException(message: String) extends Exception(message)

case class StateTransitionException(message: String) extends Exception(message)

case class WorkflowDependengcyException(message: String) extends Exception(message)

case class WorkflowTerminatedException(message: String) extends Exception(message)

case class WorkflowDefineException(message: String) extends Exception(message)