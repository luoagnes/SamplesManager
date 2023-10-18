package com.tencent.openrc.MSM.CS.operation

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import com.tencent.openrc.MSM.CS.util.IO.{printlog,saveAsText}
import com.tencent.dp.util.{DataUtil, HdfsUtil, IoUtil}
import com.tencent.dp.conf.InputConf
import com.tencent.openrc.MSM.CS.conf.{DataConf,FeatureConf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType,LongType}
import com.tencent.openrc.MSM.CS.conf.Configure
import com.tencent.openrc.MSM.CS.common.Constant
import org.apache.spark.sql.functions.col
import com.tencent.openrc.MSM.CS.util.SampleOperator
import org.apache.spark.rdd.RDD
import com.tencent.openrc.MSM.CS.util.FeatureDiscretize.{linearSearch, equiFreqDiscretization, equiValDiscretization}
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Date
import java.io.File
import org.apache.spark.storage.StorageLevel
//import scala.collection.mutable.Map



object Datahandler {
  /**
   * normal the InputConf part attr
   *
   * @param input_conf : InputConf
   * @param dataDate   : the data case date
   */
  def HandleDataPart(input_conf: InputConf, dataDate: String): Unit = {
    if (input_conf.parts == "-1") input_conf.parts = "p_" + dataDate
    if (input_conf.parts != "-1" && input_conf.parts != "last") input_conf.parts = input_conf.parts.split(",").map(r => {
      if (!r.contains("p_")) "p_" + r else r
    }).mkString(",")
  }


  /**
   * load action rdd and trabsfer it to Dataframe
   *
   * @param spark     : org.apache.spark.sql.SparkSession
   * @param configure : configure object
   */

  def loadActionDF(spark: SparkSession, configure: Configure, dataDate: String): DataFrame = {
    // handle part
    HandleDataPart(configure.action_input_conf, dataDate)

    // read data
    val (rawRDD, rawSchema) = IoUtil.loadRdd(spark, configure.action_input_conf, configure.tdw_user, configure.tdw_passwd)

    // 离散化
    val discretize_feature_list = configure.context_feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      val featureType = term.feature_type
      val is_discretize = term.is_discretize
      val featureIndex = rawSchema.indexOf(fieldName)
      (featureName, fieldName, featureType, is_discretize, featureIndex)
    }).filter(r => r._3 == Constant.FEATURETYPE_NUMERIC).filter(r => r._4 == Constant.DISCRETIZE_YES).map(r => (r._5, r._2))

    var targetdf= rawRDD
    if (discretize_feature_list.length > 0) {
      val item_discretize_path=configure.root_path+"/discretize/"+configure.action_input_conf.tbl_name
      targetdf = discretizeTable(spark: SparkSession, rawRDD, discretize_feature_list,item_discretize_path)
    }
    printlog("discretize over !!!")
    printlog("discretize features list: " + discretize_feature_list.mkString(","))

    // 编码
    val category_feature_list = configure.context_feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      val featureType = term.feature_type
      val is_encode = term.is_encode
      val featureIndex=rawSchema.indexOf(fieldName)
      (featureName, fieldName, featureType, is_encode,featureIndex)
    }).filter(r => r._3 == Constant.FEATURETYPE_CATEGORY).filter(r => r._4 == Constant.FEATUREENCODE_YES).map(r => (r._5,r._2))
    printlog("category_feature_list is: " + category_feature_list.mkString(","))


    if(category_feature_list.length > 0){
      val index_path=configure.root_path + "/index/" + configure.action_input_conf.tbl_name
      targetdf= EncodeField(spark, rawRDD, category_feature_list,index_path)
    }

    val dataRDD = targetdf.map { x => Row(x: _*) }
    val schema = StructType(rawSchema.map(ele => StructField(ele, StringType, nullable = false)).toList)

    var dataDF = spark.createDataFrame(dataRDD, schema)

    val renameSchema = configure.context_feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      (featureName, fieldName)
    }).toMap

    for (item <- renameSchema) {
      val oldName = item._2
      val newName = item._1
      dataDF = dataDF.withColumnRenamed(oldName, newName)

    }

    dataDF.show()
    dataDF
  }

  def EncodeField(spark: SparkSession, dataDF: RDD[Array[String]], category_feature_list: Array[(Int,String)], idPath: String): RDD[Array[String]] = {
    val IndexSchema = StructType(List(
      StructField("fieldName", StringType, nullable = false),
      StructField("fieldValue", StringType, nullable = false),
      StructField("fieldIndex", LongType, nullable = false)

    ))

    val dataRdd=dataDF.flatMap(item=>{
      category_feature_list.map(term=>{
        val field_index=term._1
        val field_name=term._2
        ((field_name,item(field_index)),1L)
      })
    }).reduceByKey(_+_)

    //val fieldLibrPath = idPath
    var flage=0
    var idMap=Map("" -> -1L)
    if (HdfsUtil.isExistHdfsFile(idPath)) {
      printlog(idPath+" is Exist !")
      val fieldLibDF0 = spark.sparkContext.textFile(idPath).filter(line=>line.split(",").length >2).persist(StorageLevel.DISK_ONLY)
      printlog("index data lines: "+fieldLibDF0.count().toString)
      if (fieldLibDF0.count() > 1) {
        val fieldLibrdd = fieldLibDF0.map(line => ((line.split(",")(0), line.split(",")(1)), line.split(",")(2).toLong))
        val idmaxbc=spark.sparkContext.broadcast(fieldLibrdd.map(_._2).collect().max)
        val addindexRdd=dataRdd.leftOuterJoin(fieldLibrdd).filter(_._2._2.isEmpty).map(line=>line._1).zipWithIndex().map(line=>(line._1, line._2+idmaxbc.value))
        printlog("add index data: "+addindexRdd.count().toString)
        val AddIndexDF = spark.createDataFrame(addindexRdd.map(line => Row(line._1._1, line._1._2, line._2)), IndexSchema)
        saveAsText(AddIndexDF, idPath)
        printlog("save all index data into "+idPath)

        val allIndexRdd2=addindexRdd.union(fieldLibrdd)

//        HdfsUtil.rmHdfsFile(fieldLibrPath)
//        printlog("after rm, file exist ? "+ HdfsUtil.isExistHdfsFile(fieldLibrPath).toString)

//        val finalRdd=allIndexRdd2.map(line=>Array(line._1._1, line._1._2, line._2).mkString(","))
//        finalRdd.saveAsTextFile(fieldLibrPath)

        idMap=allIndexRdd2.map(line=>(line._1._1+"_"+line._1._2,line._2)).collect().toMap
        flage=1
      }
    }

    if(flage==0){
      printlog(idPath+" is not exist !")
      val addindexRdd=dataRdd.zipWithIndex().map(line=>(line._1._1,line._2))
      printlog("all index data lines: "+addindexRdd.count().toString)

      val AddIndexDF = spark.createDataFrame(addindexRdd.map(line => Row(line._1._1, line._1._2, line._2)), IndexSchema)
      saveAsText(AddIndexDF, idPath)
      printlog("save all index data into "+idPath)

//      HdfsUtil.rmHdfsFile(fieldLibrPath)
//      addindexRdd.map(line=>Array(line._1._1, line._1._2, line._2).mkString(",")).saveAsTextFile(fieldLibrPath)
//
      idMap=addindexRdd.map(line=>(line._1._1+"_"+line._1._2,line._2)).collect().toMap
    }

    val index2feaMapbc=spark.sparkContext.broadcast(category_feature_list.toMap)
    val idMapbc=spark.sparkContext.broadcast(idMap)

    printlog("start to feature id mapping1")
    dataDF.map(item=> {
      val n = item.length - 1
      val index2feaMap = index2feaMapbc.value
      val indexarray = (0 to n).toArray

      indexarray.map(id => {
        val fieldname = index2feaMap.getOrElse(id, "-1")
        (fieldname + "_" + item(id), item(id))
      }).map(term => idMapbc.value.getOrElse(term._1, term._2).toString)
    })

  }

  /**
   * handle single one feature data, contains discretize
   *
   * @param spark             : org.apache.spark.sql.SparkSession
   * @param configure         : configure object
   * @param feature_data_conf : DataConf object, single one feature data,general is a tdw table
   */

  def handleFeatureData(spark: SparkSession, configure: Configure, feature_data_conf: DataConf): (DataFrame, List[Column]) = {
    // feature data abnormal debug
    if (feature_data_conf.input_conf.isEmpty()) {
      printlog(feature_data_conf.input_conf.tbl_name + feature_data_conf.input_conf.path + " is empty.")
      return null
    }

    // read ini feature rdd from source
    val (raw_rdd, raw_schema) = IoUtil.loadRdd(spark, feature_data_conf.input_conf, configure.tdw_user, configure.tdw_passwd)
    printlog("feats data: " + feature_data_conf.input_conf.tbl_name + feature_data_conf.input_conf.path)
    printlog(raw_rdd.take(5).map(line => line.mkString(",")).mkString("\n"))
    printlog("schema: " + raw_schema.mkString(","))

    // check the validation of id field
    val id_idx = raw_schema.indexOf(feature_data_conf.id_field)
    if (id_idx < 0) {
      printlog("id idx is smaller then zero")
      printlog("id field: " + feature_data_conf.id_field)
      return null
    }

    // 离散化
    val discretize_feature_list = feature_data_conf.feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      val featureType = term.feature_type
      val is_discretize = term.is_discretize
      val featureIndex = raw_schema.indexOf(fieldName)
      (featureName, fieldName, featureType, is_discretize, featureIndex)
    }).filter(r => r._3 == Constant.FEATURETYPE_NUMERIC).filter(r => r._4 == Constant.DISCRETIZE_YES).map(r => (r._5, r._2))

    var targetdf0 = raw_rdd
    if (discretize_feature_list.length > 0) {
      val item_discretize_path=configure.root_path+"/discretize/"+feature_data_conf.input_conf.tbl_name
      targetdf0=discretizeTable(spark: SparkSession, raw_rdd, discretize_feature_list, item_discretize_path)
    }
    printlog("discretize over !!!")
    printlog("discretize features list: "+discretize_feature_list.mkString(","))


    // 编码
    // category feature handle
    val category_feature_list = feature_data_conf.feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      val featureType = term.feature_type
      val is_encode = term.is_encode
      val featureIndex=raw_schema.indexOf(fieldName)
      (featureName, fieldName, featureType, is_encode,featureIndex)
    }).filter(r => r._3 == Constant.FEATURETYPE_CATEGORY).filter(r => r._4 == Constant.FEATUREENCODE_YES).map(r => (r._5,r._2))


    if(category_feature_list.length > 0) {
      val index_path=configure.root_path + "/index/" + feature_data_conf.input_conf.tbl_name
      targetdf0 = EncodeField(spark, targetdf0, category_feature_list, index_path)
    }
    printlog("encoding feature over !!!")
    printlog("encoded category feature list is: " + category_feature_list.mkString(","))

    val schema = StructType(raw_schema.map(r => StructField(r, StringType)).toList)
    val targetdf = spark.createDataFrame(targetdf0.map(line => Row(line: _*)), schema)

    val renameSchema = feature_data_conf.feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      (featureName, fieldName)
    }).toMap

    val id_field = feature_data_conf.id_field
    val from_id = feature_data_conf.from_id
    val to_id = feature_data_conf.to_id
    printlog("id_field: " + id_field + ", from_id: " + from_id + ", to_id: " + to_id)
    printlog("id features: " + configure.id_features.mkString(","))
    printlog("")

    //select target fields list
    val use_fields_str = configure.id_features.filter(DataUtil.isLegalString).map(term => (term, renameSchema.getOrElse(term, "empty"))).filter(_._2 != "empty").map(_._2) ++ Array(id_field)
    printlog("use_fields: " + use_fields_str.mkString(","))
    val use_fields = use_fields_str.map(term => col(term)).toList
    //    val schema = StructType(raw_schema.map(term => StructField(term, StringType)).toList)
    //    val targetdf = spark.createDataFrame(raw_rdd.map(line => Row(line: _*)), schema)
    val featureDF = targetdf.select(use_fields: _*) //.withColumnRenamed(id_field, from_id)
    featureDF.show()

    // id handle
    var resultDF = if (from_id != to_id) {
      printlog("id mapping start !!!")
      //IdMapping.idMapping(sc, featureDF, from_id, to_id)
      featureDF
    } else featureDF

    // feature handling: discretize
    resultDF.show()
    printlog("id mapping over !!!")

    val userBasicInfoCnt = resultDF.count
    printlog(s"userBasicInfoCnt=$userBasicInfoCnt")

    for (item <- renameSchema) {
      val oldName = item._2
      val newName = item._1
      resultDF = resultDF.withColumnRenamed(oldName, newName)

    }

    printlog("-----after change field -----")
    resultDF.show()

    val new_field_list = configure.id_features.filter(DataUtil.isLegalString).map(term => (term, renameSchema.getOrElse(term, "empty"))).filter(_._2 != "empty").map(ele => col(ele._1)).toList
    (resultDF, new_field_list)
  }

  /**
   * join all feature
   *
   * @param spark      : org.apache.spark.sql.SparkSession
   * @param configure  : configure object
   * @param traingData : action data
   */

  def FeatureCollect(spark: SparkSession, configure: Configure, traingData: DataFrame, dataDate: String): DataFrame = {
    var comFeatsDF = traingData

    for (data_conf <- configure.data_conf_list) {
      HandleDataPart(data_conf.input_conf, dataDate)
      val (eachFeatsDF, featsCols) = handleFeatureData(spark, configure, data_conf)

      val id_field = data_conf.join_field
      val leftkey = "t1." + id_field
      val rightkey = "t2." + data_conf.id_field
      //val use_fields =(configure.id_features.filter(DataUtil.isLegalString) ++ Array(id_field)).map(term=>(term, raw_schema.indexOf(term))).filter(_._2 >= 0).map(term=>col(term._1)).toList

      val index = featsCols.indexOf(col(data_conf.id_field))

      val featsList = comFeatsDF.columns.map(ele => {
        if (ele == id_field) col("t1." + id_field)
        else col(ele)
      }).toList ++ featsCols

      printlog("id fields: " + id_field + "; index: " + index)
      printlog("ini DF fields: " + comFeatsDF.columns.mkString(","))
      printlog("send features fields: " + featsCols.map(ele => ele.toString()).mkString(","))
      printlog("final select fields: " + featsList.map(ele => ele.toString()).mkString(","))

      // 关联用户基础画像
      comFeatsDF = comFeatsDF.as("t1").join(eachFeatsDF.as("t2"), col(leftkey) === col(rightkey), "left").select(featsList: _*).persist()

      val traingDataWithBasicCnt = comFeatsDF.count
      println(s"traingDataWithBasicCnt=$traingDataWithBasicCnt")
      comFeatsDF.show()
    }

    val finalFeature0=(configure.id_features++Array(configure.action_label_field))
    printlog("final select field list: "+ finalFeature0.mkString(","))
    val finalFeature=finalFeature0.map(term=>col(term)).toList
    val finalcomFeatsDF=comFeatsDF.select(finalFeature:_*)
    finalcomFeatsDF.show()

    finalcomFeatsDF
  }

  /**
   * join all feature
   *
   * @param spark
   * @param configure : configure object
   * @param joinDF    : action data
   */
  def SampleSave(spark: SparkSession, configure: Configure, joinDF: DataFrame): Unit = {
    // handle label imbalance
    printlog("start to handle label imbalance !!!")
    printlog("rate for the_max_label / the_min_label is " + configure.labelrate.toString)
    val samplesobj = new SampleOperator()
    val samplesDF = samplesobj.SampleByField(1, joinDF, "label", configure.labelrate)
    printlog("label imbalance handled over !!!")
    printlog("sample dataframe lines num is :" + samplesDF.count().toString)
    val schema = joinDF.schema

    // dataset split
    printlog("start to split dataset to train, val, test !!!")
    printlog("train:val:test = " + Array(configure.sample_split_train_rate, configure.sample_split_val_rate, configure.sample_split_test_rate).mkString(":"))
    val split_array = Array(configure.sample_split_train_rate, configure.sample_split_val_rate, configure.sample_split_test_rate)
    val splitDFArray = samplesDF.randomSplit(split_array)
    printlog("split dataset over!!!")
    printlog("the dataset num is: " + splitDFArray.length.toString)
    val trainDataset = splitDFArray(0).toDF()
    printlog("the train dataset samples num is: " + trainDataset.count().toString)
    val valDataset = splitDFArray(1).toDF()
    printlog("the val dataset samples num is: " + valDataset.count().toString)
    val testDataset = splitDFArray(2).toDF()
    printlog("the test dataset samples num is: " + testDataset.count().toString)

    // save path
    printlog("start to write path !!!")
    writePath(spark, configure, trainDataset, valDataset, testDataset, schema)
    printlog("write path over !!!")
  }

  /**
   * join all feature
   *
   * @param spark
   * @param configure    : configure object
   * @param trainDataset : action data
   * @param valDataset
   * @param testDataset
   * @param schema
   * @param DateStr
   */
  def writePath(spark: SparkSession, configure: Configure, trainDataset: DataFrame, valDataset: DataFrame, testDataset: DataFrame, schema: StructType, DateStr: String = new SimpleDateFormat("yyyyMMddHH").format(new Date)): Unit = {
    if (configure.sample_isText) {
      val trainPath = configure.root_path + DateStr + "train_text"
      if (HdfsUtil.isExistHdfsFile(trainPath)) HdfsUtil.rmHdfsFile(trainPath)
      trainDataset.rdd.map(_.mkString(",")).saveAsTextFile(trainPath)
      printlog("write train path: " + trainPath)

      val valPath = configure.root_path + DateStr + "val_text"
      if (HdfsUtil.isExistHdfsFile(valPath)) HdfsUtil.rmHdfsFile(valPath)
      valDataset.rdd.map(_.mkString(",")).saveAsTextFile(valPath)
      printlog("write val path: " + valPath)

      val testPath = configure.root_path + DateStr + "test_text"
      if (HdfsUtil.isExistHdfsFile(testPath)) HdfsUtil.rmHdfsFile(testPath)
      testDataset.rdd.map(_.mkString(",")).saveAsTextFile(testPath)
      printlog("write test path: " + testPath)
    }

    if (configure.sample_isTFRecord) {
      val trainDF: DataFrame = spark.createDataFrame(trainDataset.rdd, schema)
      val trainPath = configure.root_path + DateStr + "trainTFR"
      if (HdfsUtil.isExistHdfsFile(trainPath)) HdfsUtil.rmHdfsFile(trainPath)
      trainDF.write.format("tfrecords").option("recordType", "Example").save(trainPath)
      printlog("write train tfrecord path: " + trainPath)

      val valDF: DataFrame = spark.createDataFrame(valDataset.rdd, schema)
      val valPath = configure.root_path + DateStr + "valTFR"
      if (HdfsUtil.isExistHdfsFile(valPath)) HdfsUtil.rmHdfsFile(valPath)
      valDF.write.format("tfrecords").option("recordType", "Example").save(valPath)
      printlog("write val tfrecord path: " + valPath)

      val testDF: DataFrame = spark.createDataFrame(testDataset.rdd, schema)
      val testPath = configure.root_path + DateStr + "testTFR"
      if (HdfsUtil.isExistHdfsFile(testPath)) HdfsUtil.rmHdfsFile(testPath)
      testDF.write.format("tfrecords").option("recordType", "Example").save(testPath)
      printlog("write test tfrecord path: " + testPath)
    }
  }




  def getDiscretizeBounds(discretize_features:Array[(String,Int)],boundary_num: Int, input: RDD[String], discretizeMethod: String): Array[(String, Array[Float])] = {
//    feature_rdd.persist(StorageLevels.MEMORY_AND_DISK_SER)
    printlog("count before distribution data start !!!")
    val distribution0 = input.flatMap(line => {
      val tmp=line.split(",")
      discretize_features.map(term => {
        val feature_name = term._1
        val field_index = term._2
        val value =
          try {
            tmp(field_index).toFloat
          }
          catch {
            case _: Throwable => 0.0F
          }
        (feature_name, value)
      }).filter(_._2 != 0)
    })
    printlog("count before distribution data over !!!")
    val distribution=distribution0.map(item => (item, 1L)).reduceByKey(_ + _).collect().groupBy(_._1._1).mapValues(_.map(item => (item._1._2, item._2)))
//    printlog("start to print distribution!!!")
//    distribution.take(5).map(line=>line._1+"@@"+line._2.mkString(":")).foreach(printlog)

    val boundarys = discretize_features.map(item => {
      val feature_name = item._1

      val each_distribution=distribution.getOrElse(feature_name, null)
      if(each_distribution == null || each_distribution.isEmpty) (feature_name, Array[Float]())
      else {
        val boundary = discretizeMethod match {
          case Constant.DISCRETIZE_EQUI_FREQ => equiFreqDiscretization(each_distribution, boundary_num)
          case Constant.DISCRETIZE_EQUI_VAL => equiValDiscretization(each_distribution, boundary_num)
          case _ => null
        }
        (feature_name, boundary)
      }
    }).filter(item => item._2 != null && item._2.nonEmpty)

//    val check_boundarys: String = boundarys.
//      map(i => "index: %d   %s".format(i._1, i._2.map(j => "%.3f".format(j)).mkString(","))).mkString("|")
//    printlog("boundarys: " + check_boundarys)
    boundarys
  }

  def getDiscretizeBounds(discretize_features: Array[String], boundary_num: Int, input: DataFrame, discretizeMethod: String): Array[(String, Array[Float])] = {
    printlog("count before distribution data start !!!")
    val distribution0 = input.rdd.flatMap(line => {
      val fieldarray=discretize_features.map(term => {
        val value =
          try {
            line.getAs[Float](term)
          }
          catch {
            case _: Throwable => 0.0F
          }
        (term, value)
      }).filter(_._2 != 0)

      fieldarray
    })

    printlog("count before distribution data over !!!")

    val distribution = distribution0.map(item => (item, 1L)).reduceByKey(_ + _).collect().groupBy(_._1._1).mapValues(_.map(item => (item._1._2, item._2)))
    //    printlog("start to print distribution!!!")
    //    distribution.take(5).map(line=>line._1+"@@"+line._2.mkString(":")).foreach(printlog)

    val boundarys = discretize_features.map(item => {
      val feature_name = item

      val each_distribution = distribution.getOrElse(feature_name, null)
      if (each_distribution == null || each_distribution.isEmpty) (feature_name, Array[Float]())
      else {
        val boundary = discretizeMethod match {
          case Constant.DISCRETIZE_EQUI_FREQ => equiFreqDiscretization(each_distribution, boundary_num)
          case Constant.DISCRETIZE_EQUI_VAL => equiValDiscretization(each_distribution, boundary_num)
          case _ => null
        }
        (feature_name, boundary)
      }
    }).filter(item => item._2 != null && item._2.nonEmpty)

    //    val check_boundarys: String = boundarys.
    //      map(i => "index: %d   %s".format(i._1, i._2.map(j => "%.3f".format(j)).mkString(","))).mkString("|")
    //    printlog("boundarys: " + check_boundarys)
    boundarys
  }

  def getBoundary(sc: SparkContext, discretizePath: String, boundary_num: Int, input: RDD[String], idx: Int, discretizeMethod: String): Array[Float] = {
    var boundarys = Array[Float]()

    // 计算分布
    val distribution = input.filter(line=>line.split(",")(idx).forall(e=>e.isDigit)).map(line => {
      val tmp = line.split(",")
      val v = tmp(idx)
      (v, 1)
    }).reduceByKey((x, y) => x + y).filter(line=> line._1.forall(e=>e.isDigit)).map(line => {
      val key = line._1.toFloat
      (key, line._2.toLong)
    }).collect()

    // 计算边界
    boundarys =
      discretizeMethod match {
        case Constant.DISCRETIZE_EQUI_FREQ => equiFreqDiscretization(distribution, boundary_num)
        case Constant.DISCRETIZE_EQUI_VAL => equiValDiscretization(distribution, boundary_num)
        case _ => null
      }

    val boundarysRdd = sc.parallelize(boundarys.map(_.toString))
    if (HdfsUtil.isExistHdfsFile(discretizePath)) HdfsUtil.rmHdfsFile(discretizePath)
    boundarysRdd.saveAsTextFile(discretizePath)

    boundarys
  }

  def discretizeData(sc: SparkContext, input: RDD[String], boundaryArray: Map[Int, Array[Float]]): RDD[String] = {
    val boundaryArraybc = sc.broadcast(boundaryArray)
    input.filter(line=>line.split(",").length > 1).map(line => {
      val tmp = line.split(",")
      val iniArray = tmp.zipWithIndex
      iniArray.map(term => {
        val index = term._2
        val iniv = term._1

        val value = if (boundaryArraybc.value.contains(index)) {
          val boundary = boundaryArraybc.value.getOrElse(index,Array[Float]())
          if(iniv.length > 1 && iniv.forall(e=>e.isDigit || e=='.') && boundary.length > 0) {
              linearSearch(boundary, iniv.toFloat).toString
          }else{
            if(iniv.length==1 && iniv.forall(e=>e.isDigit) && boundary.length > 0) {
              linearSearch(boundary, iniv.toFloat).toString
            }else "-1"
          }
        } else iniv
        value
      }).mkString(",")
    })
  }

  def discretizeData(sc: SparkContext, input: DataFrame, boundaryArray: Map[String, Array[Float]]): RDD[String] = {
    val boundaryArraybc = sc.broadcast(boundaryArray)
    val feature_list=input.schema.toList
    val feature_name_list=input.schema.toList.map(_.name)
    println("----------------------------print dataframe datatype -----------------------------")
    println(feature_list.map(_.dataType.typeName).mkString(","))
    println("----------------------------print dataframe datatype -----------------------------")

    input.rdd.map(line => {
         feature_list.map(fea => {
        fea.dataType.typeName match {
          case "float" =>{
            val iniv = line.getAs[Float](fea.name)
            val value = if (iniv == null){
              "-1"
            }else{
              if(boundaryArraybc.value.contains(fea.name)){
                val boundary = boundaryArraybc.value.getOrElse(fea.name, Array[Float]())
                (linearSearch(boundary, iniv) + 1).toString
              } else iniv.toString
            }
            value
          }

          case "long" => {
            val iniv = line.getAs[Long](fea.name)
            val value = if (iniv == null) {
              "-1"
            }else{
              if (boundaryArraybc.value.contains(fea.name)) {
                val boundary = boundaryArraybc.value.getOrElse(fea.name, Array[Float]())
                (linearSearch(boundary, iniv) + 1 ).toString
              } else iniv.toString
            }
            value
          }

          case "double" => {
            val iniv = line.getAs[Double](fea.name)
            val value = if (iniv == null){
              "-1"
            }else{
              if (boundaryArraybc.value.contains(fea.name)) {
                val boundary = boundaryArraybc.value.getOrElse(fea.name, Array[Float]())
                (linearSearch(boundary, iniv) + 1).toString
              } else iniv.toString
            }
            value
          }

          case _ => line.getAs[String](fea.name)
        }
      }).mkString(",")
    })
  }

  def discretizeTable(spark: SparkSession, dataDF: RDD[Array[String]], discretize_feature_list: Array[(Int, String)], idPath: String): RDD[Array[String]] = {
    val sc = spark.sparkContext
    val discretize_feature_list_bc=sc.broadcast(discretize_feature_list.map(term=>(term._2, term._1)).toMap)

    val boundaryArray= sc.textFile(idPath).filter(line => line.split(",").length > 1).map(line=>{
      val tmp=line.split(",")
      val feat_name=tmp(0)
      val boundary_list=tmp(1).split(";").map(_.toFloat)
      (feat_name, boundary_list)
    }).map(line=>(discretize_feature_list_bc.value.getOrElse(line._1,-1), line._2)).filter(_._1 >=0).collect().toMap

    val inputDF = dataDF.map(line => line.mkString(","))
    val discretizeRDD0 = discretizeData(sc, inputDF, boundaryArray)//.map(line=>line.split(","))
    discretizeRDD0.map(line=>line.split(","))
  }

  def getFeatureIndex(encode_features: Array[(String, Int)], input: RDD[String], delta:Int=5000): RDD[String]= {
    val distribution0 = input.flatMap(line => {
      val tmp = line.split(",")
      encode_features.map(term => {
        val feature_name = term._1
        val field_index = term._2
        val value =
          try {
            tmp(field_index)
          }
          catch {
            case _: Throwable => "-1"
          }

        (feature_name, value)
      }).filter(_._2 != "-1").flatMap(line=>{
        val feature_name=line._1
        val value=line._2
        if(value.contains(',') || value.contains(';') || value.contains(':')){
          value.split(",|;|:").map(term=>(feature_name, term))
        }
        else Array((feature_name, value))
      })
    })

    val distribution = distribution0.map(item => (item, 1L)).reduceByKey(_ + _).filter(line=>line._2 > delta).map(
      line=>(line._1._1, (line._1._2, line._2))).groupByKey().flatMap(
        line=>{
          val feature_name=line._1
          val index_array=line._2.filter(term=>term._2 > 1000).toArray.sortWith((x,y)=>x._2 > y._2).zipWithIndex
          index_array.map(term=>Array(feature_name, term._1._1, term._1._2, term._2).mkString(","))
      })
    distribution
  }

  def calc_bounary_and_index(spark: SparkSession, input_conf: InputConf, feature_conf_list: Array[FeatureConf], boundary_num: Int, discretizeMethod: String, tdw_user: String, tdw_passwd: String, root_path:String): Unit = {
    input_conf.parts=""
    val (rawRDD, rawSchema) = IoUtil.loadRdd(spark, input_conf, tdw_user, tdw_passwd)
    val dataRDD=rawRDD.map(line=>line.mkString(",")).persist(StorageLevel.DISK_ONLY)

    // get discretize feature list
    val discretize_feature_list = feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      val featureType = term.feature_type
      val is_discretize = term.is_discretize
      val featureIndex = rawSchema.indexOf(fieldName)
      (featureName, fieldName, featureType, is_discretize, featureIndex)
    }).filter(r => r._3 == Constant.FEATURETYPE_NUMERIC).filter(r => r._4 == Constant.DISCRETIZE_YES).map(r => (r._2,r._5))

    if(discretize_feature_list.length > 0) {
      val boundarys = getDiscretizeBounds(discretize_feature_list, boundary_num, dataRDD, discretizeMethod).map(item => {
        val key = item._1
        val boundary = item._2.mkString(";")
        Array(key, boundary).mkString(",")
      })

      val item_discretize_path = root_path + "/discretize/" + input_conf.tbl_name
      if (HdfsUtil.isExistHdfsFile(item_discretize_path)) HdfsUtil.rmHdfsFile(item_discretize_path)
      spark.sparkContext.parallelize(boundarys).saveAsTextFile(item_discretize_path)
    }

    // get encode feature list
    val category_feature_list = feature_conf_list.map(term => {
      val fieldName = term.value_field
      val featureName = term.feature_name
      val featureType = term.feature_type
      val is_encode = term.is_encode
      val featureIndex = rawSchema.indexOf(fieldName)
      (featureName, fieldName, featureType, is_encode, featureIndex)
    }).filter(r => r._3 == Constant.FEATURETYPE_CATEGORY).filter(r => r._4 == Constant.FEATUREENCODE_YES).map(r => (r._2,r._5))

    if(category_feature_list.length > 0){
        val indexRDD = getFeatureIndex(category_feature_list, dataRDD)
        val n = indexRDD.count()
        if (n > 2) {
          val item_index_path = root_path + "/index/" + input_conf.tbl_name
          if (HdfsUtil.isExistHdfsFile(item_index_path)) HdfsUtil.rmHdfsFile(item_index_path)
          indexRDD.saveAsTextFile(item_index_path)
        }
    }


  }
}