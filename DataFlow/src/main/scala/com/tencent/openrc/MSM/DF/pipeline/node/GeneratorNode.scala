/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline.node

import com.tencent.openrc.MSM.DF.pipeline.ExecutorException
/*
 * 无输入单输出的计算节点
 */
abstract class GeneratorNode[O](id: String) extends BaseNode(id) {

  override def addInNode(node: BaseNode): Unit = {
    throw ExecutorException("GeneratorNode has no InNodes")
  }

  override def addOutNode(node: BaseNode): Unit = {
    if (outNodes.isEmpty) {
      super.addOutNode(node)
    } else {
      throw ExecutorException("UnaryNode only have one OutNode")
    }
  }

  override def execute(): Unit = {
    val output = doExecute()
    this.result = output
  }

  def doExecute(): O

  override def toString(): String = s"GeneratorNode(${fid}, $gid)"
}
