/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

class Pipeline(id: String, job: Job) extends Workflow(id, job) {

  var input: Any = _

  override final def execute(): Unit = {

    /*
    children.foreach(n => {
      if (n.isInstanceOf[StartNode]) {
        n.asInstanceOf[StartNode].input = input
      }
    })
     */

    engine.execute(this)
    /*
    if (this.isCompleted()) {
      children.foreach(n => {
        if (n.isInstanceOf[EndNode]) {
          result = n.asInstanceOf[EndNode].result.get
        }
      })
    }
     */
  }
}
