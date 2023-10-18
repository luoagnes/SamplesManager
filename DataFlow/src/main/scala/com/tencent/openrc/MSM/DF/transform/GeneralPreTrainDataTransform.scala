package com.tencent.openrc.MSM.DF.transform
/**
 * Copyright 2021, Tencent Inc.
 * All rights reserved.
 *
 * @author zebodong <zebodong@tencent.com>
 */

import java.util.HashMap
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import com.tencent.openrc.MSM.DF.util.SparkRegistry.spark
import com.tencent.openrc.MSM.DF.util.SparkRegistry.spark.implicits._
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import com.tencent.openrc.MSM.CS.util.IO.saveTFRecord
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
import scala.collection.JavaConversions._
import com.tencent.openrc.MSM.CS.util.IO.{printlog, rmHdfs, saveAsText}

import java.util
import scala.collection.immutable.HashSet
import scala.collection.mutable


class GeneralPreTrainDataTransform(id: String) extends UnaryNode[DataFrame, Unit](id) {
  private val sc = SparkRegistry.sc
  override def doExecute(input: DataFrame): Unit = {
    val datetime = getPropertyOrThrow("dateTime")
    val delimiter = getPropertyOrThrow("delimiter").trim
    val feature_index_path=getProperty("feature_index_path")
    val outputPath = getPropertyOrThrow("outputPath") + "/" + datetime
    val source = "presamples"
    val partNum = getPropertyOrThrow("partNum").toInt


    printlog("generalPreTrainDataTransform source: " + source)
    printlog("del path: " + outputPath)
    printlog("delimiter: " + delimiter)

    val field_array = input.schema.fields.map(_.name).filterNot(_ == "wuid")
    printlog(field_array.mkString(delimiter))
    val field_array_bc=spark.sparkContext.broadcast(field_array)

    printlog("input data lines:"+input.count().toString)

    // 打平字段
    val data= input.rdd.flatMap(line => {
      val wuid = line.getAs[String]("wuid").reverse
      field_array_bc.value.map(fieldname => {
        val feats = line.getAs[String](fieldname)
        (wuid, fieldname, feats)
      }).filterNot(x => x._2 == null).filterNot(x => x._1.trim == "").filterNot(x => x._1.trim == "-1").filter(x=>x._3.split(delimiter).length >= 3 && x._3.split(delimiter).length < 30)
    }).persist(StorageLevel.MEMORY_AND_DISK)
    printlog("after flatten, the data lines :"+data.count().toString)
    data.take(10).map(line=>line._1+","+line._2+","+line._3).foreach(println)

    // 计算各个词出现的频次
    val staticData: RDD[((String, String), Long)] = data.flatMap(line => {
      line._3.split(delimiter).map(term => (line._2, term))
    }).map(x => (x, 1L))
      .reduceByKey(_ + _)
      .cache()
    printlog("in data, the feat value cnt: " + staticData.count())

    //过滤掉低频标签
    var ini_Vocab=sc.parallelize(Array[((String,String),Long)]())
    var max_index=0L
    if (feature_index_path.isDefined && feature_index_path.nonEmpty) {
      //加载词典
      val ini_vocab_rdd= feature_index_path.get.split(";").map(path => sc.textFile(path).filter(_.split(",").length > 2)).reduce(_.union(_))
        if(! ini_vocab_rdd.isEmpty()) {
          ini_Vocab=ini_vocab_rdd.map(line => {
            val tmp = line.split(",")
            val feat_name = tmp(0)
            val feat_value = tmp(1)
            val feat_index = tmp(2).toLong
            ((feat_name, feat_value), feat_index)
          }).cache()
        }
      printlog("load vocab success !!!")
      printlog("vocab size: " + ini_Vocab.count().toString)

      max_index=ini_Vocab.map(_._2).max()
    }

    val filterData0=staticData.leftOuterJoin(ini_Vocab).map(term => (term._1._1, (term._1._2, term._2))).groupByKey().flatMap(line => {
      val fieldname = line._1
      val index_list=line._2.filter(item=>item._2._2.isDefined && item._2._2.nonEmpty).map(item=>((item._1,item._2._1), item._2._2.get))
      val value_list = line._2.filter(item=>item._2._1 <= 10000000 && item._2._1 >= 10000).filter(item=>item._2._2.isEmpty).filter(item=> ! item._2._2.isDefined).toArray.sortWith((x, y) => x._2._1 > y._2._1).zipWithIndex
        .map(item=>((item._1._1,item._1._2._1), item._2+max_index+1))
      //value_list.map(item => ((fieldname, item._1._1), (item._1._2, item._2)))
      (index_list ++ value_list).map(item=>((fieldname, item._1._1),(item._1._2, item._2)))
    }).filter(_._2._2 > -1).cache()

    var feature_index_save_path="hdfs://ss-teg-4-v2/user/datamining/agneswluo/AMS/Experiemnt/brand/index/"+ getPropertyOrThrow("outputPath").split("/").last
    if(feature_index_path.isDefined && feature_index_path.nonEmpty){
      feature_index_save_path=feature_index_path.get
    }
    rmHdfs(feature_index_save_path)
    filterData0.map(item=>item._1._1+","+item._1._2+""+item._2._2.toString).saveAsTextFile(feature_index_save_path)

    val filterData=filterData0.filter(x => x._2._1 <= 10000000 && x._2._1 >= 10000).cache()
    printlog("after index, the feat value cnt: : " + filterData.count())


    //过滤行为
    val vocab=filterData.map(line=>(line._1,line._2._2)).collect() // ((feature_name, feature_value),index)
    val bcVocab = SparkRegistry.sc.broadcast(vocab.map(_._1))
    val Mergedf= data
      .map(x => {
        val tag = x._3
        val fieldname=x._2
        val wuid = x._1
        val tag_array=tag.split(delimiter).filter(term=>bcVocab.value.contains((fieldname,term)))
        (wuid,fieldname, tag_array)
      }).filter(line=>line._3.length > 0).map(line=>(line._1, line._2, line._3.mkString(delimiter))).toDF("wuid", "fieldname", "tags")
    //Mergedf.show(20, truncate = true)

    // 正样本
    val vocabNewMap = SparkRegistry.sc.broadcast(new HashMap[(String, String), Long](vocab.toMap))
    val posData = GeneralPreTrainDataTransform.generatePosData(Mergedf, delimiter, vocabNewMap).persist(StorageLevel.MEMORY_AND_DISK)
    //posData.sample(false, 0.01).select("sample_tags", "select_tag", "label").show(5, truncate = true)
    println("posData count:" + posData.count().toString)
    posData.show(20, truncate = false)
    rmHdfs(outputPath + "/Pos_text")
    posData.map(f => {
      Array(
        f.getAs[String]("wuid"),
        f.getAs[String]("fieldname"),
        f.getAs[String]("tags"),
        f.getAs[String]("sample_tags"),
        f.getAs[String]("select_tag"),
        f.getAs[Int]("label")
      ).mkString(",")
    }).rdd.saveAsTextFile(outputPath + "/Pos_text")

    // 负样本
    // 分字段计算总频率
    val filterData2 = filterData.map(line => (line._1._1, line._2._1)).reduceByKey((x, y) => x + y)
    //filterData2.take(10).map(term => term._1 + " : " + term._2.toString).foreach(printlog)

    // 负采样词库
    val weightVocab = filterData.map(line => (line._1._1, (line._1._2, line._2._1, line._2._2))).join(filterData2).map(line => {
      val feat_name = line._1
      val feat_value = line._2._1._1
      val feat_num = line._2._1._2
      val feat_index = line._2._1._3
      val sum_num = line._2._2
      val rate = feat_num * 1.0 / sum_num
      ((feat_name, feat_value), (feat_index, rate))
    }).collect().toMap
    //      .filter(_._4 > 0.00001).flatMap(line => {
    //      val feat_name = line._1
    //      val feat_value = line._2
    //      val feat_index = line._3
    //      val num = line._4 * 100000
    //      Array.range(0, num.toInt).map(term => (feat_name, (feat_value, feat_index)))
    //    }).groupByKey().map(line => (line._1, line._2.toArray)).collect().toMap
    printlog("negative sampling vocab size: "+weightVocab.toList.length.toString)

    val negVocabMap=SparkRegistry.sc.broadcast(new HashMap[(String,String), (Long,Double)](weightVocab))

    //负采样
    val negativeDF = GeneralPreTrainDataTransform.generateNegDataV2(
      posData,
      delimiter,
      negVocabMap
    ).map(f=> {
      Array(
        f.getAs[String]("wuid"),
        f.getAs[String]("fieldname"),
        f.getAs[String]("tags"),
        f.getAs[String]("sample_tags"),
        f.getAs[String]("select_tag"),
        f.getAs[Int]("label")
      ).mkString(",")
    }).rdd
      //.persist(StorageLevel.MEMORY_AND_DISK)
    printlog("negative df success over !!!")
    //negativeDF.show(20, truncate = true)
    //
    rmHdfs(outputPath+"/Nega_text")
    negativeDF.saveAsTextFile(outputPath+"/Nega_text")
    //saveTFRecord(spark,outputPath+"/Nega_text",negativeDF)

//    val finalData= negativeDF.union(posData).filter(f => {
//      f.getAs[String]("select_tag").length > 0 &&
//        f.getAs[String]("sample_tags").length > 0
//    }).map(f=>{
//      Array(
//        f.getAs[String]("wuid"),
//        f.getAs[String]("fieldname"),
//        f.getAs[String]("tags"),
//        f.getAs[String]("sample_tags"),
//        f.getAs[String]("select_tag"),
//        f.getAs[String]("label")
//      ).mkString(",")
//    }).rdd
//
//    val unextendPath=s"$outputPath/tf_unextended_text"
//    rmHdfs(unextendPath)
//    //val ss=SparkRegistry.spark
//    //val unExtendSchema=posData.schema
//    finalData.saveAsTextFile(unextendPath)
//    //saveTFRecord(ss, unextendPath,finalData.rdd,unExtendSchema)
//
//    printlog("union success !!!")
//
//    val extended_schema = StructType(
//      field_array.flatMap(term => {
//        Array(
//          StructField(s"${term}_tags", StringType),
//          StructField(s"${term}_sample_tags", StringType),
//          StructField(s"${term}_select_tag", StringType),
//          StructField(s"${term}_label", StringType)
//        )
//      }) ++ Array(StructField("wuid", StringType)).toList
//    )
//
//    val zeroValue: ArrayBuffer[String] = ArrayBuffer[String]()
//    val seqOp: (ArrayBuffer[String],  (String, String)) => ArrayBuffer[String] = (accumulator, value) => accumulator += value._1 + "_" + value._2
//    val combOp: (ArrayBuffer[String], ArrayBuffer[String]) => ArrayBuffer[String] = (accumulator1, accumulator2) => accumulator1 ++= accumulator2
//
//
//    val featureData=finalData.rdd.map(
//      x=>(
//        x.getAs[String]("wuid"),
//        (
//          x.getAs[String]("fieldname"),
//          Array(
//            x.getAs[String]("tags"),
//            x.getAs[String]("sample_tags"),
//            x.getAs[String]("select_tag"),
//            x.getAs[Int]("label").toString
//          ).mkString("@")
//        )
//      )
//    ).aggregateByKey(zeroValue)(seqOp,combOp).map(line=>{
//      val wuid=line._1
//      val fieldMap=line._2.map(term=>(term.split("_")(0), term.split("_")(1))).toMap
//      field_array_bc.value.map(term=>fieldMap.getOrElse(term, "-1")).mkString("@")+"@"+wuid
//    }).map(line=>Row.fromSeq(line.split("@").toSeq))//.toDF(field_name_list:_*)
//
//    val finalDF=spark.createDataFrame(featureData,extended_schema)
//
//    printlog("extended over !!!")
//    GeneralPreTrainDataTransform.saveResAsText(finalDF, field_array, delimiter, outputPath, 0, 0)
//    finalDF.show(20, truncate = true)
  }
}


object GeneralPreTrainDataTransform {

  val table_size: Int = 100000000

  def InitUnigramTable(vocab: Array[((String,String), Long)], delimiter:String): Array[Int] = {
    var train_words_pow = 0.0
    val vocab_size = vocab.length
    var d1 = 0.0
    val power = 0.75
    val table = new Array[Int](table_size)
    for (a <- 0 until vocab_size)
      train_words_pow += math.pow(vocab(a)._2, power)

    println("train_words_pow:" + train_words_pow)

    if(vocab_size >0) {
      var i = 0
      d1 = math.pow(vocab(i)._2, power) / train_words_pow
      for (a <- 0 until table_size) {
        table(a) = i
        if (a / (1.0 * table_size) > d1) {
          i += 1
          d1 += math.pow(vocab(i)._2, power) / train_words_pow
        }
        if (i >= vocab_size) i = vocab_size - 1
      }
    }
    println("table:" + table.take(1000).map(_.toString).mkString(delimiter))
    table
  }

  def addIndex(df: DataFrame): DataFrame = SparkRegistry.spark.createDataFrame(
    // Add index
    df.rdd.zipWithIndex.map { case (r, i) => Row.fromSeq(r.toSeq :+ i) },
    // Create schema
    StructType(df.schema.fields :+ StructField("_index", LongType, false))
  )

  def generateSampleData(df: DataFrame,
                         delimiter:String,
                         fieldName:String,
                         bcTable: Broadcast[Array[Int]],
                         bcVocab: Broadcast[Array[String]]): DataFrame = {
    df.flatMap(f => {
      val wuid = f.getAs[String]("wuid")
      val tags = f.getAs[String](fieldName).split(delimiter)
      val table = bcTable.value
      val vocab = bcVocab.value
      val result = new ArrayBuffer[(String, String, String, String, Int)]
      if (tags.length <= 1) {
        Array[(String, String, String, String, Int)]().iterator
      } else {
        for (i <- 0 until math.min(Random.nextInt(tags.length) + 5, 10)) {
          val sample = Random.shuffle(tags.toList).take(2).toArray
          result.append((wuid, tags.mkString(delimiter), sample(0), sample(1), 1))
          val negative = Random.nextInt(2)
          for (d <- 0 to negative) {
            var continue = true
            var word = ""
            while (continue) {
              val target = table(Random.nextInt(table_size))
              word = vocab(target)
              if (tags.contains(word)) {
                continue = true
              } else {
                continue = false
              }
            }
            result.append((wuid, tags.mkString(delimiter), sample(0), word, 0))
          }
        }
        result.iterator
      }
      //result.iterator
    })
      .toDF("wuid", "fieldname", s"${fieldName}_tags", s"${fieldName}_sample_tags", s"${fieldName}_select_tag", s"${fieldName}_label")
  }

  def generatePosData(
                       df: DataFrame,
                       delimiter:String,
                       bcVocabMap: Broadcast[java.util.HashMap[(String,String), Long]]
                     ): DataFrame = {

    val newDF = df
      .flatMap(f => {
        //val vocabSize = bcVocabMap.value.size() + 1
        //val vocabMapTmp = bcVocab.value.zip(bcVocab.value.indices).toMap
        //val vocabMap = bcVocabMap.value
        val wuid = f.getAs[String]("wuid")
        val fieldname=f.getAs[String]("fieldname")
        val tags = f.getAs[String]("tags").split(delimiter)
        val tagsSize=tags.length

        val posCount = if (tagsSize >= 4) {
          1 + math.min(Random.nextInt(tagsSize - 3), 1) //3-6
        } else {
          1
        }

        val result = new ArrayBuffer[(String, String, String, String, String, Int)]
        for (i <- 0 until posCount) {
          val maxLength = 50
          var sampleCount = if(tagsSize >= 2){
            math.min(Random.nextInt(tagsSize - 1) + 1, maxLength)
          } //1-6
          else{
            1
          }

          // sample

          //val seqsize=tagsSize - 1
          var sampleTags= mutable.Set[String]()
          var selectTag= mutable.Set[String]()
          val currVocab=bcVocabMap.value//.filter(item=>item._1._1 == fieldname)

          if(tagsSize > 2) {
            var sampleSeq = tags.to[ListBuffer]
            //val selectSeq = tags.to[ListBuffer]
            //val ids1 = Iterator.continually(Random.nextDouble()).take(sampleSeq.size).toList.zipWithIndex
            var i=sampleCount
            while(i>0 && sampleSeq.nonEmpty) {
              if (sampleSeq.length == i) {
                sampleTags = sampleSeq.to[mutable.Set]
                i = 0
              } else {
                //val ids1 = (0 to i).map(term => Random.nextInt(sampleSeq.size - 1))
                val ids1 = Iterator.continually(Random.nextInt(sampleSeq.size - 1)).take(i)
                //val token_list1 =Random.shuffle(sampleSeq).take(i)
                val token_list1 = ids1.map(id => sampleSeq(id)).to[ListBuffer]
                //.zipWithIndex
                //.sortWith((x, y) => x._1 > y._1)
                //.toList//.map(_._2)
                //val token_list1 = ids1.to[HashSet].map(id => sampleSeq(id))
                val new_token_list1: mutable.Seq[String] = token_list1.map(token => currVocab.getOrElse((fieldname, token), -1)).distinct.map(_.toString).filterNot(token => sampleTags.contains(token))

                sampleTags ++= new_token_list1
                sampleSeq --= token_list1
                i -= new_token_list1.length
              }
            }
            // mask postive samples
            val maskNum = math.min(4,tags.length)
            //如果tags很小的话，可能陷入循环
            //i=maskNum
            //val ids = Iterator.continually(Random.nextDouble()).take(selectSeq.size).toList.zipWithIndex
            i=maskNum
            var selectSeq = tags.to[ListBuffer]
            while(i>0 && selectSeq.nonEmpty) {
              if(selectSeq.length == i){
                selectTag = selectSeq.to[mutable.Set]
                i=0
              } else {
                //val ids = (0 to i).map(term => Random.nextInt(selectSeq.size - 1))
                val ids = Iterator.continually(Random.nextInt(selectSeq.size - 1)).take(i)
                //val token_list =Random.shuffle(selectSeq).take(i)
                val token_list = ids.map(id => selectSeq(id)).to[ListBuffer]
                //.zipWithIndex
                //.sortWith((x, y) => x._1 > y._1)
                //.toList//.map(_._2).toList
                //val token_list = ids.to[HashSet].map(id => selectSeq(id))
                val new_token_list: mutable.Seq[String] = token_list.map(token => currVocab.getOrElse((fieldname, token), -1)).distinct.map(_.toString).filterNot(token => selectTag.contains(token))
                selectTag ++= new_token_list
                selectSeq --= token_list
                i -= new_token_list.length
              }
            }
          }

          result.append(
            (
              wuid,
              fieldname,
              tags.mkString(delimiter),
              sampleTags
                //.map(vocabMap.getOrElse(_, vocabSize).toString)
                .mkString(delimiter),
              //vocabMap.getOrElse(selectTag, vocabSize).toString,
              selectTag.mkString(delimiter),
              1
            )
          )
        }
        result.filter(item=>item._4.nonEmpty && item._5.nonEmpty && item._4.trim.nonEmpty && item._5.trim.nonEmpty).iterator
      })
      .toDF("wuid", "fieldname","tags", "sample_tags", "select_tag", "label")

    newDF
  }

  def Binarysearch(arr: ListBuffer[Double], value: Double): Int = {

    var left = 0
    var right = arr.size - 1
    var mid = -1

    while (left <= right) {
      mid = left + ((right - left) >> 1)

      if (arr(mid) > value) {
        right = mid - 1
      } else if (arr(mid) < value) {
        left = mid + 1
      } else {
        return mid
      }
    }
    mid
  }


  def generateNegDataV2(
                         df: DataFrame,
                         delimiter:String,
                         vocab: Broadcast[java.util.HashMap[(String,String), (Long,Double)]]
                       ): DataFrame = {
    df.flatMap(x => {
      val tags = x.getAs[String]("tags").split(delimiter).to[HashSet]
      val vocabMap=vocab.value

      val wuid = x.getAs[String]("wuid")
      val fieldname=x.getAs[String]("fieldname")
      val sample_tags = x.getAs[String]("sample_tags")

      var person_vocabMap=vocabMap.filter(_._1._1==fieldname).filterNot(term=>tags.contains(term._1._2)).to[ListBuffer]
        .sortWith((x,y)=>x._2._2 < y._2._2)
      var vocabCumRate=person_vocabMap.scanLeft(0.0)((x,y)=> x + y._2._2)
      var interval=1-vocabCumRate.max

      // 计算
      val vocab_size = person_vocabMap.size
      val negative=math.min(math.max(1, Random.nextInt(2)), vocab_size)

      val result = new ArrayBuffer[(String, String, String, String, String, Int)]
      if(vocab_size > 0) {
        for (d <- 0 to negative) {
          //var person_vocab = person_vocabMap.clone()
          val maskNum = math.min(4, person_vocabMap.length)
          //          val ids = Iterator.continually(Binarysearch(vocabCumRate,Random.nextDouble())).take().toSet.take(maskNum)
          var negaWords = mutable.Set[String]()//token_list.to[HashSet]
          var i=maskNum
          while(i>0 && person_vocabMap.size > 0){
            val ids=Iterator.continually(Binarysearch(vocabCumRate,math.max(0,Random.nextDouble()-interval))).take(i)
            val token_list=ids.filter(term=>term > 0 ).to[HashSet].map(term=>person_vocabMap(term-1)._2._1.toString)
            val new_token_list=token_list.filterNot(term=>negaWords.contains(term))
            negaWords ++=new_token_list
            i-=new_token_list.size

            person_vocabMap=person_vocabMap.filterNot(term=>token_list.contains(term._2._1.toString))
            vocabCumRate=person_vocabMap.scanLeft(0.0)((x,y)=> x + y._2._2)
            interval=1-vocabCumRate.max
          }

          result.append(
            (
              wuid,
              fieldname,
              tags.mkString(delimiter),
              sample_tags,
              //vocabMap.getOrElse(word, vocabSize).toString,
              //              word,
              negaWords.mkString(delimiter),
              0
            )
          )
        }
      }
      result.filter(item=>item._4.nonEmpty && item._5.nonEmpty && item._4.trim.nonEmpty && item._5.trim.nonEmpty).iterator
    })
      .toDF("wuid", "fieldname", "tags", "sample_tags", "select_tag", "label")
  }

  def addShuffleColumn(df: DataFrame, column: String): DataFrame = {
    import org.apache.spark.sql.functions.{col, rand}

    val df_shuffled = addIndex(
      df.select(col(column).as("shuffle_column"))
        .orderBy(rand)
    )
    val newDf = addIndex(df)
      .join(df_shuffled, Seq("_index"))
      .drop("_index")

    newDf.show()
    newDf
  }

  def saveResAsText(
                     input: DataFrame,
                     fieldList:Array[String],
                     delimiter:String,
                     outputPath: String,
                     label_num: Int,
                     label_idx: Int
                   ): Unit = {

    printlog("start to save as text !!!")
    //saveAsText(input, s"$outputPath/text")

    //input.saveAsTextFile(s"$outputPath/text")
    input.schema.map(_.name).foreach(printlog)
    //val schema =input.schema
    //val fieldList=input.schema.map(_.name)
//    val schema =StructType(
//      fieldList.flatMap(term=>{
//        Array(
//          StructField(s"${term}_tags", StringType),
//          StructField(s"${term}_sample_tags", StringType),
//          StructField(s"${term}_select_tag", StringType),
//          StructField(s"${term}_label", StringType)
//        )
//      })++Array(StructField("wuid", StringType)).toList
//    )
//
//    val tfDataTmp= input.rdd.map(x=>{
//
//      val new_feats=fieldList.flatMap(field=>Array(
//        x.getAs[String](s"${field}_tags"),
//        x.getAs[String](s"${field}_sample_tags"),
//        x.getAs[String](s"${field}_select_tag"),
//        x.getAs[String](s"${field}_label")
//      )) :+ x.getAs[String]("wuid").reverse
//
//      Row(new_feats:_*)
//      //new GenericRow(new_feats.toArray)
//      //Row.fromSeq(new_feats)
//
//    })


//    printlog("tfDataTmp over !!!")
//    val tfData = tfDataTmp//.map(f => (Random.nextInt(21474830), f)).partitionBy(new HashPartitioner(6000)).map(f => f._2)

    val dataArray = input.randomSplit(Array(0.9, 0.1))
    val trainData=dataArray(0).toDF(input.schema.map(_.name):_*)
    val testData=dataArray(1).toDF(input.schema.map(_.name):_*)
    val ss=SparkRegistry.spark
    val trainpath=s"$outputPath/tf_train"
    val testath=s"$outputPath/tf_test"
    testData.show(5)

    rmHdfs(trainpath)
    saveTFRecord(ss, trainpath, trainData)
    rmHdfs(testath)
    saveTFRecord(ss, testath, testData)

  }
}