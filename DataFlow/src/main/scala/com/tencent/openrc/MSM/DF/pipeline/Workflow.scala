/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.collection.mutable
import com.tencent.openrc.MSM.DF.pipeline.node.BaseNode

/**
 * TODO: add scheduler and realtime features
 */
class Workflow(id: String, job: Job) extends Executable with Context {

  private val log = LoggerFactory.getLogger(this.getClass)

  /*
   * workflow的所有子节点都应该包含在children中，其依赖关系也应包含在其中
   * 目前暂时不支持依赖节点在workflow之外的情况
   */
  val ARG_JOB_NAME = "_#_JOB_NAME_#_"
  val AUTHORS = "_#_AUTHORS_#_"
  val children: mutable.Set[BaseNode] = mutable.Set()

  var engine: WorkflowEngine = new WorkflowEngine

  private val args: mutable.HashMap[String, String] = mutable.HashMap()

  addArg(ARG_JOB_NAME, job.name)
  log.info(s"Workflow(${id}) added args: ${ARG_JOB_NAME} -> ${job.name}")
  addArg(AUTHORS, job.authors)
  log.info(s"Workflow(${id}) added args: ${AUTHORS} -> ${job.authors}")

  def addChild(child: BaseNode): Unit = {
    this.children.add(child)
    child.workflow = this
  }

  override def init(): Boolean = {

    /* haoxiliu
    if (children.count(_.isInstanceOf[StartNode]) == 0) {
      val start = new StartNode()
      children.filter(_.inNodes.size == 0).foreach(start.addOutNode(_))
      this.addChild(start)
    }
    if (children.count(_.isInstanceOf[EndNode]) == 0) {
      val end = new EndNode()
      children.filter(_.outNodes.size == 0).foreach(end.addInNode(_))
      this.addChild(end)
    }
    */

    val isInit = children.map(_.init()).count(!_) == 0 && super.init()
    if (isInit) {
      this.executorInitialized()
    }
    isInit
  }

  override def execute(): Unit = {
    // scalastyle:off println
    // println("execute in workflow")
    engine.execute(this)
  }

  override def isReady: Boolean = {
    this.executorGetReady()
    this.isExecutorReady()
  }

  override def isCompleted: Boolean = {
    this.isExecutorCompeleted()
  }

  override def isTerminated: Boolean = {
    this.isExecutorTerminated()
  }

  override def fail(): Unit = {
    this.executorFailed()
    super.fail()
  }

  override def preExecute(): Unit = {
    isReady match {
      case false => throw ExecutorNotReadyException("")
      case true =>
    }
    this.executorWorking()
    super.preExecute()
  }

  override def postExecute(): Unit = super.postExecute()

  def setArgs(map : HashMap[String, String]): Unit = {
    args.clear()
    map.foreach(e => args.put(e._1, e._2))
  }

  def addArg(key: String, value: String): Unit = {
    args.put(key, value)
  }

  def getArg(key: String): Option[String] = {
    args.get(key)
  }

  def getArgOrThrow(key: String, errorMessage: String = ""): String = {
    val arg = getArg(key)
    if (arg.isDefined) {
      arg.get
    } else {
      val realMsg = if (errorMessage.isEmpty) {
        s"missing $key"
      } else {
        errorMessage
      }
      throw new IllegalArgumentException(realMsg)
    }
  }

  def getArgs: Map[String, String] = {
    args.toMap
  }

  override def toString: String = {
    val d = children.flatMap(n => {
      n.outNodes.map(o => {
        s"[${o}-->${n}]"
      })
    }).mkString(",")
    s"Workflow(${id}, children={$children}, dependency={$d})"
  }
}
