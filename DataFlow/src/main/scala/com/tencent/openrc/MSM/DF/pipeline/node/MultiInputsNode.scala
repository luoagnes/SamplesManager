/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline.node

import com.tencent.openrc.MSM.DF.pipeline.{ExecutorException,WorkflowDefineException}
import scala.collection.mutable

/*
 * 多输入单输出的计算节点
 */
abstract class MultiInputsNode[I, O](id: String) extends BaseNode(id) {

  val inputConfigMap: mutable.Map[String, BaseNode] = mutable.Map[String, BaseNode]()

  override def addOutNode(node: BaseNode): Unit = {
    if (outNodes.isEmpty) {
      super.addOutNode(node)
    } else {
      throw ExecutorException("MultiInputsNode only have one OutNode")
    }
  }

  protected def getInput: Map[String, I] = {
    inputConfigMap
      .filter(_._2.result != None)
      .map(kv => {
        if (!kv._2.result.get.isInstanceOf[I]) {
          throw new WorkflowDefineException(s"${kv._2}, output type is not right")
        }
        (kv._1, kv._2.result.get.asInstanceOf[I])
      }).toMap
  }

  protected def needToExecute(input: Map[String, I]): Boolean = input.nonEmpty

  override def execute(): Unit = {
    // scalastyle:off println
    // println(s"execute in Streamed ${this.getClass.getName}")
    val input: Map[String, I] = this.getInput
    if (needToExecute(input)) {
      val output = doExecute(input)
      this.result = output
    }
  }

  def doExecute(input: Map[String, I]): O

  override def toString(): String = s"MultiInputsNode(${fid}, $gid)"
}

