package com.tencent.openrc.MSM.CS.conf

import scala.xml.Node

/**
 * Created by groverli on 2016/5/31.
 */
class FeatureConf extends Serializable {

  // feature property
  var feature_name: String = ""
  var feature_type: String = ""
  var feature_index: Int = -1
  var dtype=""

  // field name
  var value_field : String = ""
  var score_field : String = ""

  // feature operator

  var operator: String = ""

  // discretize config
  var discretize : Boolean = false
  var discretize_type : String = ""
  var discretize_argv : Int = 10
  var is_encode:Int = 0
  var is_discretize:Int=0

  // top config
  var top : Boolean = false
  var top_argv : Int = -1

  def parse(n: Node) = {
    val raw_feature_name = (n \ "@feature_name").toString()
    val raw_feature_type = (n \ "@feature_type").toString()
    val raw_dtype =  (n \ "@type").toString()
    feature_name = if (raw_feature_name != null && raw_feature_name.nonEmpty) raw_feature_name else ""
    feature_type = if (raw_feature_type != null && raw_feature_type.nonEmpty) raw_feature_type else ""
    dtype=if (raw_dtype != null && raw_dtype.nonEmpty) raw_dtype else ""

    val raw_value_field = (n \ "@value_field").toString()
    val raw_score_field = (n \ "@score_field").toString()
    value_field = if (raw_value_field != null && raw_value_field.nonEmpty) raw_value_field else ""
    score_field = if (raw_score_field != null && raw_score_field.nonEmpty) raw_score_field else ""

    val raw_is_encode=(n \ "@isencode").toString()
    is_encode= if(raw_is_encode != null && raw_is_encode.nonEmpty) raw_is_encode.toInt else 0

    val raw_is_discretize=(n \ "@isdiscretize").toString()
    is_discretize= if(raw_is_discretize != null && raw_is_discretize.nonEmpty) raw_is_discretize.toInt else 0

    val raw_discretize_type = (n \ "@discretize").toString()
    discretize_type = if (raw_discretize_type != null && raw_discretize_type.nonEmpty) raw_discretize_type else ""
    if (discretize_type.nonEmpty) {
      discretize = true
      val raw_discretize_argv = (n \ "@discretize_argv").toString()
      discretize_argv = if (raw_discretize_argv != null && raw_discretize_argv.nonEmpty) raw_discretize_argv.toInt else 10
    }

    val raw_top = (n \ "@top").toString()
    if (raw_top != null && raw_top.nonEmpty) {
      top_argv =
        try {
          raw_top.toInt
        }
        catch {
          case _: Throwable => -1
        }
      if (top_argv > 0)
        top = true
    }

    val raw_operator = (n \ "@operator").toString()
    if (raw_operator != null && raw_operator.nonEmpty)
      operator = raw_operator
  }
}

