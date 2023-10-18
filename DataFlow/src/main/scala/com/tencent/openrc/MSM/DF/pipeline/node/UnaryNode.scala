/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline.node


/*
 * 单输入单输出的计算节点
 */
abstract class UnaryNode[I, O](id: String) extends BaseNode(id) {

  override def addInNode(node: BaseNode): Unit = {
    if (inNodes.isEmpty) {
      super.addInNode(node)
    } else {
      throw new Exception("UnaryNode only have one InNode")
    }
  }

  override def addOutNode(node: BaseNode): Unit = {
    if (outNodes.isEmpty) {
      super.addOutNode(node)
    } else {
      throw new Exception("UnaryNode only have one OutNode")
    }
  }

  protected def getInput(): Option[I] = {
    val results = inNodes.map(bn => {
      if (!bn.result.isInstanceOf[I]) {
        throw new Exception(s"$bn, output type is not right")
      }
      bn
    }).filter(_.result.isInstanceOf[I]).toList.flatMap(_.result).map(_.asInstanceOf[I])
    if(results.nonEmpty){
      Option(results.head)
    } else {
      Option.empty
    }
  }

  protected def getInNode: Option[BaseNode] ={
    if (inNodes.nonEmpty) {
      Some(inNodes.head)
    } else {
      Option.empty
    }
  }

  override def execute(): Unit = {
    // scalastyle:off println
    // println(s"execute in Streamed ${this.getClass.getName}")
    val input: Option[I] = this.getInput()
    val finalInput = if (input.isDefined) {
      input.head
    } else {
      val unit = ()
      assert(unit.isInstanceOf[I], s"node ${this.getClass.getSimpleName} must have input node")
      unit.asInstanceOf[I]
    }
    val output = doExecute(finalInput)
    this.result = output
  }

  def doExecute(input: I): O

  override def toString(): String = s"UnaryNode(${fid}, $gid)"
}

