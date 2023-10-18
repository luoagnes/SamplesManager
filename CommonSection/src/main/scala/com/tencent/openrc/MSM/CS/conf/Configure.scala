package com.tencent.openrc.MSM.CS.conf

import java.io.InputStream
import com.tencent.dp.conf.InputConf
import com.tencent.openrc.MSM.CS.conf.DataConf
import com.tencent.dp.util.DataUtil
import com.tencent.openrc.MSM.CS.common.Constant

import scala.xml.XML


/**
 * Created by groverli on 2016/5/9.
 * user item均有都情况
 */
class Configure extends Serializable {
  // task config
  var task_type: String = Constant.TASKTYPE_TRAIN // predict or train task
  var data_type: String = Constant.DATATYPE_ONLYUSER // onlyuser or pairs

  // tdw config
  var tdw_user: String = Constant.TDW_USER
  var tdw_passwd: String = Constant.TDW_PASSW

  // sample
  var sample_split_train_rate = 1.0
  var sample_split_test_rate = 0.0
  var sample_split_val_rate = 0.0

  var labelrate=2
  var root_path=""

//  var train_data_path = ""
//  var val_data_path = ""
//
//  var test_data_path = ""
//  var predict_data_path=""

  var sample_isTFRecord = true
  var sample_isLibSVM = true
  var sample_isText = true

  //action data
  var action_input_conf: InputConf = new InputConf
  var action_label_field = ""
  var action_ids:Array[String]=new Array[String](0)

  var context_feature_conf_list:Array[FeatureConf] = new Array[FeatureConf](0)

  var data_conf_list: Array[DataConf] = new Array[DataConf](0)

  var comb_features: Array[(String, Array[String])] = new Array[(String, Array[String])](0)
  var id_features: Array[String] = new Array[String](0)

  var is_debug: Boolean = false
  var calc_corr: Boolean = false

  def load(config_stream: InputStream): Unit = {
    val config = XML.load(config_stream)

    val user = (config \ "tdw" \ "user" \ "@name").toString()
    val passwd = (config \ "tdw" \ "passwd" \ "@name").toString()
    if (user != null && user.nonEmpty)
      tdw_user = user
    if (passwd != null && passwd.nonEmpty)
      tdw_passwd = passwd

    val raw_task_type = (config \ "task_type" \ "@name").toString()
    if (raw_task_type != null && raw_task_type.nonEmpty)
      task_type = raw_task_type

    data_type=(config \ "data_type" \ "@name").toString()

    //sample
    labelrate=(config \ "sample" \ "labelrate" \ "@name").toString().toInt

    val raw_sample_split_train_rate = (config \ "sample" \ "split" \ "train" \ "@name").toString()
    if (raw_sample_split_train_rate != null && raw_sample_split_train_rate.nonEmpty)
      sample_split_train_rate = raw_sample_split_train_rate.toFloat

    val raw_sample_split_val_rate = (config \ "sample" \ "split" \ "val" \ "@name").toString()
    if (raw_sample_split_val_rate != null && raw_sample_split_val_rate.nonEmpty)
      sample_split_val_rate = raw_sample_split_val_rate.toFloat

    val raw_sample_split_test_rate = (config \ "sample" \ "split" \ "test" \ "@name").toString()
    if (raw_sample_split_test_rate != null && raw_sample_split_test_rate.nonEmpty)
      sample_split_test_rate = raw_sample_split_test_rate.toFloat

    val raw_save_path = (config \ "sample" \ "save_path" \ "root_path" \ "@name").toString()
    if (raw_save_path != null && raw_save_path.nonEmpty)
      root_path = raw_save_path

//    val raw_val_save_path = (config \ "sample" \ "save_path" \ "val_path" \ "@name").toString()
//    if (raw_val_save_path != null && raw_val_save_path.nonEmpty)
//      val_data_path = raw_val_save_path
//
//    val raw_test_save_path = (config \ "sample" \ "save_path" \ "test_path" \ "@name").toString()
//    if (raw_test_save_path != null && raw_test_save_path.nonEmpty)
//      test_data_path = raw_test_save_path
//
//    val raw_predict_save_path = (config \ "sample" \ "save_path" \ "predict_path" \ "@name").toString()
//    if (raw_predict_save_path != null && raw_predict_save_path.nonEmpty)
//      predict_data_path = raw_predict_save_path

    val raw_sample_isTFRecord = (config \ "sample" \ "save_type" \ "is_TFRecord").text
    if (raw_sample_isTFRecord != null && raw_sample_isTFRecord.nonEmpty)
      sample_isTFRecord = raw_sample_isTFRecord.toBoolean

    val raw_sample_isLibSVM = (config \ "sample" \ "save_type" \ "is_LibSVM").text
    if (raw_sample_isLibSVM != null && raw_sample_isLibSVM.nonEmpty)
      sample_isLibSVM = raw_sample_isLibSVM.toBoolean

    val raw_sample_isText = (config \ "sample" \ "save_type" \ "is_Text").text
    if (raw_sample_isText != null && raw_sample_isText.nonEmpty)
      sample_isText = raw_sample_isText.toBoolean

    //action
    if ((config \ "action_data" \ "input").nonEmpty)
      action_input_conf.parse((config \ "action_data" \ "input").head)

    val raw_action_label_field = (config \ "action_data" \ "label_name" \ "@name").toString()
    if (raw_action_label_field != null && raw_action_label_field.nonEmpty)
      action_label_field = raw_action_label_field

    context_feature_conf_list = (config \ "action_data" \ "contenxt_features" \ "feature").toArray.map(node => {
      val feature_conf = new FeatureConf
      feature_conf.parse(node)
      feature_conf
    })

    action_ids=(config \ "action_data" \ "ids" \ "id").toArray.map(node => {
      val id = (node \ "@name").toString()
      id
    })


    data_conf_list=(config \ "feature_collect" \ "feature_data").toArray.map(node => {
      val data_conf = new DataConf
      data_conf.parse(node)
      data_conf
    })

    val comb_features_conf = (config \ "gen_feature" \ "features").
      filter(x => (x \ "@name").text == "comb_features")

    comb_features = (comb_features_conf \\ "feature").toArray.map(node => {
      val name = (node \ "@name").toString()
      val feats = (node \ "@dependencies").toString()
      (name, feats)
    }).filter(item => DataUtil.isLegalString(item._1) && DataUtil.isLegalString(item._2)).map(item => {
      (item._1, item._2.split(",|;", -1).filter(term => DataUtil.isLegalString(term)).sorted.mkString(","))
    }).groupBy(_._2).mapValues(item => item.sortBy(_._1).head._1).toArray.
      map(item => (item._2, item._1.split(",", -1))).filter(_._2.length >= 2)


    val id_features_conf = (config \ "gen_feature" \ "features").
      filter(x => (x \ "@name").text == "id_features")

    id_features = (id_features_conf \\ "feature").toArray.map(node => {
      val name = (node \ "@name").toString()
      name
    }).filter(_ != null).filter(_.nonEmpty).distinct.sorted


    // other
    val debug_raw = (config \ "debug" \ "@name").toString()
    if (debug_raw != null && debug_raw.nonEmpty && debug_raw.toLowerCase.equals("true"))
      is_debug = true

    val raw_correlation = (config \ "correlation" \ "@name").toString()
    if (raw_correlation != null && raw_correlation.nonEmpty && raw_correlation.toLowerCase.equals("true"))
      calc_corr = true

    // 统一排序所有的特征
    val ini_feature_index_list = data_conf_list.flatMap(data_conf => {
      data_conf.feature_conf_list.map(feature_conf => {
        feature_conf.feature_name
      })++context_feature_conf_list.map(feature_conf => {
          feature_conf.feature_name
        })
    })

    // 所有特征的index表
    val feature_index_list = ini_feature_index_list.filter(item => DataUtil.isLegalString(item)).distinct.sorted.zipWithIndex.toMap

    data_conf_list.map(data_conf => {
      data_conf.feature_conf_list.map(feature_conf => {
        feature_conf.feature_index = feature_index_list.getOrElse(feature_conf.feature_name, -1)
      })
    })

    context_feature_conf_list.map(feature_conf => {
      feature_conf.feature_index = feature_index_list.getOrElse(feature_conf.feature_name, -1)
    })

  }
}


