/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 *
 */
package com.tencent.openrc.MSM.DF.pipeline

import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.slf4j.LoggerFactory

/**
 * 用于执行pipeline的任务
 *
 * @param pipeline
 */
class Task(
            val pipeline: Pipeline,
            batchTime: String = null,
            poolEnabled: Boolean = false,
            webhook: String = null) extends Callable[Boolean] {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def call(): Boolean = {
    try {
      if (!poolEnabled) {
        // this is the default case
        assert(batchTime == null, "batchTime must be null when poolEnabled = false")
        //beforePipelineRun(pipeline)
        pipeline.execute()
        //afterPipelineCompleted(pipeline)
        true
      } else {
        false
      }
    } catch {
      case th: Throwable =>
        val jobName = pipeline.getArgOrThrow(pipeline.ARG_JOB_NAME)
        log.error(s"${jobName}'s Task.call() failed with exception")
        afterPipelineFailed(pipeline)
        false
    }
  }


  def beforePipelineRun(pipeline: Pipeline): Unit = {
    // scalastyle:off println
    log.info(s"start to run pipeline: ${pipeline}")
  }

  def afterPipelineFailed(pipeline: Pipeline): Unit = {
    // scalastyle:off println
    log.info(s"pipeline: ${pipeline}, run failed")

  }

  def afterPipelineCompleted(pipeline: Pipeline): Unit = {
    // scalastyle:off println
    log.info(s"pipeline(${pipeline}) ends with result = ${pipeline.result}")

}
}
