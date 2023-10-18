package com.tencent.openrc.MSM.DF.util

import org.slf4j.LoggerFactory
import com.tencent.openrc.MSM.DF.pipeline.node.BaseNode


object NodeUtil {
  def getCoalescePartition(node: BaseNode): (Boolean, Int) = {
    val log = LoggerFactory.getLogger(this.getClass)
    val coalescePartition =
      node
        .getProperty("coalescePartition")
        .getOrElse("-1")
        .toInt
    log.info(s"""coalescePartition = $coalescePartition""")
    if (coalescePartition > 0) {
      (true, coalescePartition)
    } else {
      (false, -1)
    }
  }

}
