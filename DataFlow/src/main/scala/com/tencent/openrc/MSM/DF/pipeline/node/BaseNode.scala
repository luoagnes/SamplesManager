/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline.node

import com.tencent.openrc.MSM.DF.pipeline.{Context, Executable, ExecutorNotReadyException, Workflow}
import scala.collection.mutable

/*
 * 基础节点，定义节点间的依赖关系
 */
abstract class BaseNode(_fid: String) extends Executable with Context {

  // flow id 用于workflow内部的唯一标识，一般用于描述节点之间的依赖关系
  val fid: String = _fid
  // global id 用于定义全局唯一的ID，便于查找和复用
  val gid = 1//GlobalIdGenerator.gen(this.getClass.getSimpleName)

  val inNodes: mutable.Set[BaseNode] = mutable.Set()
  val outNodes: mutable.Set[BaseNode] = mutable.Set()
  var workflow: Workflow = _

  override def init(): Boolean = {
    this.executorInitialized()
    true
  }

  def addInNode(node: BaseNode): Unit = {
    this.inNodes.add(node)
    node.outNodes.add(this)
  }

  def addOutNode(node: BaseNode): Unit = {
    this.outNodes.add(node)
    node.inNodes.add(this)
  }

  override def isReady(): Boolean = {
    if (!this.isExecutorReady()) {
      val ready = inNodes.map(n => n.isCompleted()).count(!_) == 0
      if (ready) {
        this.executorGetReady()
      }
    }
    this.isExecutorReady()
  }

  override def preExecute(): Unit = {
    isReady() match {
      case false => throw ExecutorNotReadyException("")
      case true =>
    }
    this.executorWorking()
    super.preExecute()
  }

  override def fail(): Unit = {
    this.executorFailed()
    super.fail()
  }

  override def isCompleted(): Boolean = {
    this.isExecutorCompeleted()
  }

  override def isTerminated(): Boolean = {
    this.isExecutorTerminated()
  }

  override def postExecute(): Unit = {
    super.postExecute()
  }

  /**
   * 获取节点的配置参数，参数优先级如下：
   * 1、程序输入参数
   * 2、该节点的配置参数
   * 3、workflow的配置参数
   *
   * @param key
   * @return
   */
  override def getProperty(key: String): Option[String] = {
    val arg = {
      if (workflow != null) {
        val property = workflow.getArg(s"${fid}.${key}")
        if (property.isEmpty) {
          workflow.getArg(key)
        } else {
          property
        }
      } else {
        Option.empty
      }
    }
    arg.isDefined match {
      case true => arg
      case false => {
        val value = super.getProperty(key)
        value.isDefined match {
          case true => value
          case false => {
            if (workflow != null){
              workflow.getProperty(key)
            } else {
              Option.empty
            }
          }
        }
      }
    }
  }

  override def getPropertyOrThrow(key: String, errorMessage: String = ""): String = {
    val property = getProperty(key)
    if (property.isDefined) {
      property.get
    } else {
      val realMsg = if (errorMessage.isEmpty) {
        s"missing $key"
      } else {
        errorMessage
      }
      throw new IllegalArgumentException(realMsg)
    }
  }

  override def getProperties(): Map[String, String] = {
    val args: Map[String, String] = workflow.getArgs // 1.获取命令行参数
    val argsFixed = args.map(arg => {
      val key = arg._1
      val value = arg._2
      val newKey = if (key.startsWith(fid + ".")) {
        key.drop(fid.length + 1)
      } else {
        key
      }
      (newKey, value)
    })
    val globalProperties = workflow.getProperties // 2.全局参数
    val properties = super.getProperties         // 3. 节点专用参数
    val mergedMap = globalProperties ++ properties ++ argsFixed // argsFixed优先级最高
    mergedMap.foreach(tuple2 => {
      assert(getProperty(tuple2._1).get == tuple2._2)
    })
    mergedMap
  }

  def getWorkflow: Workflow = {
    workflow
  }

  def getFid: String = {
    fid
  }

  override def toString: String = s"BaseNode($fid, $gid)"
}