/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import com.tencent.openrc.MSM.DF.pipeline.node.BaseNode

class WorkflowEngine {

  // scalastyle:off
  def execute(workflow: Workflow): Unit = {
    if (!workflow.isCompleted && workflow.isReady) {
      Try {
        workflow.preExecute()
        var q = mutable.ListBuffer[BaseNode]()
        q.appendAll(workflow.children)
        while (q.nonEmpty) {
          val preCount = q.size
          q.foreach(n => {
            if (n.isReady()) {
              Try {
                n.preExecute()
                n.execute()
              } match {
                case Success(_) =>
                  n.executorCompleted()
                  n.postExecute()
                case Failure(e) =>
                  e match {
                    case e1: WorkflowTerminatedException =>
                      // workflow提前终止，正常退出
                      n.executorTerminated()
                      n.postExecute()
                    case _ =>
                      n.fail()

                  }
                  throw e

              }
            }
          })
          q = q.filter(!_.isCompleted())
          if (q.size == preCount) {
            throw WorkflowDependengcyException("")
          }
        }
      } match {
        case Success(_) =>
          workflow.executorCompleted()
          workflow.postExecute()

        case Failure(e) =>
          println(e)
          e match {
            case e1: WorkflowTerminatedException =>
              // workflow提前终止，正常退出
              // scalastyle:off println
              println(e1)
              workflow.executorTerminated()
              workflow.postExecute()
            case _ =>
              workflow.fail()
              throw e
          }
      }
    }
  }

}
