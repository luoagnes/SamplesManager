package com.tencent.openrc.MSM.CS.common

/**
 * Created by groverli on 2016/9/19.
 */
object Constant {
  val FEATURETYPE_USER_BASE="user_base_feats"
  val FEATURETYPE_USER_CNT="user_cnt_feats"
  val FEATURETYPE_USER_EXT="user_extended_feats"
  val FEATURETYPE_ITEM_BASE="item_base_feats"
  val FEATURETYPE_ITEM_CNT="user_cnt_feats"
  val FEATURETYPE_ITEM_EXT="item_extended_feats"
  val FEATURETYPE_CONTEXT="context_feats"

  val FEATURETYPE_NUMERIC = "numeric"
  val FEATURETYPE_CATEGORY = "category"
  val FEATURETYPE_MULTI = "multi"
  val FEATUREENCODE_YES=1
  val FEATUREENCODE_NO=0


  val DISCRETIZE_EQUI_FREQ = "equi_freq"
  val DISCRETIZE_EQUI_VAL = "equi_val"
  val DISCRETIZE_YES=1
  val DISCRETIZE_NO=0

  val OPERATOR_DUMMY = "dummy"
  val OPERATOR_IDENTITY = "identity"

  val TASKTYPE_TRAIN = "train"
  val TASKTYPE_VAL = "val"
  val TASKTYPE_TEST = "test"
  val TASKTYPE_PREDICT = "predict"

  val NUMERIC_VALUE = "num"

  val MULTI_SEPARATOR = ",|\\|"
  val MULTI_KEYVALUE_SEPARATOR = ":"

  val DIRNAME_COLLECT_DEBUG = "collect_debug"
  val DIRNAME_DISCRETIZE = "discretize"
  val DIRNAME_INDEX = "index"
  val DIRNAME_DATA = "data"
  val DIRNAME_EVALUATE = "evaluate"
  val DIRNAME_DUMMY_IG = "dummy_ig"
  val DIRNAME_FEATURE_DISTRIBUTION = "feature_distribution"
  val DIRNAME_FEATURE_CORRELATION = "feature_correlation"

  val FILENAME_EVALUATE = "feature_evaluate.txt"
  val FILENAME_FEATURE_COVER = "feature_cover.txt"

  val OPTIMIZE_OUTPUT_LINES = 5000

  val RESULTDIR_TFRECORD="TFRecord_format"
  val RESULTDIR_LIBSVM="LibSVM_format"
  val RESULTDIR_TEXT="Text_format"

  val RESULTDIR_TRAIN="train_data"
  val RESULTDIR_VAL="val_data"
  val RESULTDIR_TEST="test_data"

  val TDW_USER=""
  val TDW_PASSW=""

  val DATATYPE_ONLYUSER="user"
  val DATATYPE_ONLYITEM="item"
  val DATATYPE_PAIRS="pairs"

  val INIUSERIDPATH="hdfs://ss-teg-4-v2/user/datamining/agneswluo/3C/samples/user_id_index/index_list/2020_ini"
  val INIUSERIDDIR="hdfs://ss-teg-4-v2/user/datamining/agneswluo/3C/samples/user_id_index/index_list"
  val MAXINDEXPATH="hdfs://ss-teg-4-v2/user/datamining/agneswluo/3C/samples/user_id_index/max_index"

  val ID_MAPPING_CONFIG_PATH="hdfs://ss-teg-4-v2/user/datamining/agneswluo/3C/id_mapping_config.xml"

  val FIELD_SEP=","

  val FILEFORMAT_PROTOBUF = "protobuf"
  val FILEFORMAT_TEXT = "text"
  val FILEFORMAT_SEQUENCE = "sequence"

}


