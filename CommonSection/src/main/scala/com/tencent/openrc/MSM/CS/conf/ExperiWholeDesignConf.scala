package com.tencent.openrc.MSM.CS.conf

import java.io.InputStream
import scala.xml.{Node, XML}
import com.tencent.dp.conf.InputConf


class ExperiWholeDesignConf {
  // task config
  var task_type: String = "" // predict or train task

  // tdw config
  var tdw_user: String = ""
  var tdw_passwd: String = ""

  var root_path: String = ""

  // raw instance
  var root_instance_input: InputConf = new InputConf
  var root_instance_label_field: String = ""
  var root_instance_userid_field: String = ""
  var root_instance_split_field: String = ""

  var remain_label_field: Int = 0
  var remain_userid_field: Int = 0
  var remain_split_field: Int = 0

  var select_feature_list: Array[String] = new Array[String](0)
  var experiment_conf_list: Array[ExperimentConf] = new Array[ExperimentConf](0)


  def load(config_stream: InputStream): Unit = {
    val config = XML.load(config_stream)

    val user = (config \ "tdw" \ "user" \ "@name").toString()
    if (user != null && user.nonEmpty)
      tdw_user = user

    val passwd = (config \ "tdw" \ "passwd" \ "@name").toString()
    if (passwd != null && passwd.nonEmpty)
      tdw_passwd = passwd

    val raw_root_path = (config \ "path" \ "root_path" \ "@name").toString()
    if (raw_root_path != null && raw_root_path.nonEmpty)
       root_path= raw_root_path

    val raw_task_type = (config \ "task_type" \ "@name").toString()
    if (raw_task_type != null && raw_task_type.nonEmpty)
      task_type = raw_task_type


    if ((config \ "root_instance" \ "input").nonEmpty)
      root_instance_input.parse((config \ "root_instance" \ "input").head)

    val label_field = (config \ "root_instance" \ "label" \ "@name").toString()
    root_instance_label_field = if (label_field != null && label_field.nonEmpty) label_field else ""
    val islabel_field = (config \ "root_instance" \ "label" \ "@isRemain").toString()
    remain_label_field = if (islabel_field != null && islabel_field.nonEmpty) islabel_field.toInt else 0

    val userid_field = (config \ "root_instance" \ "userId" \ "@name").toString()
    root_instance_userid_field = if (userid_field != null && userid_field.nonEmpty) userid_field else ""
    val isuserid_field = (config \ "root_instance" \ "userId" \ "@isRemain").toString()
    remain_userid_field = if (isuserid_field != null && isuserid_field.nonEmpty) isuserid_field.toInt else 0

    val splitfield_name = (config \ "root_instance" \ "splitField" \ "@name").toString()
    root_instance_split_field = if (splitfield_name != null && splitfield_name.nonEmpty) splitfield_name else ""
    val issplit_field = (config \ "root_instance" \ "splitField" \ "@isRemain").toString()
    remain_split_field = if (issplit_field != null && issplit_field.nonEmpty) issplit_field.toInt else 0

    select_feature_list = (config \ "root_instance" \ "selectFeatures" \ "field").toArray.map(node => {
      val name = (node \ "@name").toString()
      name
    })

    experiment_conf_list = (config \ "experimentList" \ "experiment").toArray.map(node => {
      val data_conf = new ExperimentConf
      data_conf.parse(node)
      data_conf
    })
  }

}
