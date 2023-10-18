package com.tencent.openrc.MSM.CS.conf

import scala.xml.Node

class ExperimentConf {
  var experiment_name: String = ""
  var experiment_rate: Double = 0.25
  var postive_smaple_value_list:Array[String]=new Array[String](0)
  var negative_smaple_value_list:Array[String]=new Array[String](0)


  def parse(n: Node) = {
    val raw_experiment_name = (n \ "@name").toString()
    experiment_name = if (raw_experiment_name != null && raw_experiment_name.nonEmpty) raw_experiment_name else ""

    val raw_experiment_rate = (n \ "@splitRate").toString()
    experiment_rate = if (raw_experiment_rate != null && raw_experiment_rate.nonEmpty) raw_experiment_rate.toDouble else 0.25

    postive_smaple_value_list = (n \ "postiveSamples" \\ "fieldValue").toArray.map(node => {
      val value = (node \ "@value").toString()
      value
    })

    negative_smaple_value_list = (n \ "negativeSamples" \\ "fieldValue").toArray.map(node => {
      val value = (node \ "@value").toString()
      value
    })
  }

}
