package com.tencent.openrc.MSM.CS.conf

import com.tencent.dp.conf.InputConf
import scala.xml.Node

/**
 * Created by Administrator on 2016/5/9.
 */
class DataConf extends Serializable {

  var input_conf : InputConf = new InputConf

  var feature_conf_list: Array[FeatureConf] = new Array[FeatureConf](0)
  var name:String=""
  var time:String=""

  var id_field : String = "" // actual id field name in ini data
  var join_field : String = ""  // join action data id field
  var from_id : String = "" // ini id field
  var to_id : String = ""  // final join id field

  def parse(n: Node) = {

    input_conf.parse((n \ "input").head)
    feature_conf_list = (n \ "features" \ "feature").toArray.map(node => {
      val feature_conf = new FeatureConf
      feature_conf.parse(node)
      feature_conf
    })

    name=(n \ "@norm_name" ).toString()
    time=(n \ "@time").toString()

    val raw_id_field = (n \ "id_conf" \ "@id_field").toString()
    val raw_join_field = (n \ "id_conf" \ "@join_field").toString()
    val raw_from_id = (n \ "id_conf" \ "@from_id").toString()
    val raw_to_id = (n \ "id_conf" \ "@to_id").toString()
    id_field = if (raw_id_field != null && raw_id_field.nonEmpty) raw_id_field else ""
    join_field = if (raw_join_field != null && raw_join_field.nonEmpty) raw_join_field else ""
    from_id = if (raw_from_id != null && raw_from_id.nonEmpty) raw_from_id else ""
    to_id = if (raw_to_id != null && raw_to_id.nonEmpty) raw_to_id else ""
    if (from_id.isEmpty && to_id.isEmpty) {
      from_id = id_field
      to_id = id_field
    }
  }
} // class DataConf
