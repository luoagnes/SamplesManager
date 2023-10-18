package com.tencent.openrc.MSM.DF.util

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.gdt.log_process.log_format.ProtobufCombineInputFormat
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

object DataHandling {
  def loadData(
                format: String,
                sc: SparkContext,
                inputPath: String,
                allowInputNotExist: Boolean = false): RDD[Array[Byte]] = {
    val path = new Path(inputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val inputRDDs =
      inputPath.split(",").map(inputStr => {
        val fileStatuses = fs.globStatus(new Path(inputStr))
        val inputExist = fileStatuses != null && fileStatuses.nonEmpty
        if (inputExist) {

          format match {
            case "protobuf" =>
              sc
                .newAPIHadoopFile[LongWritable, BytesWritable, ProtobufCombineInputFormat](inputStr)
                .map(_._2.copyBytes())

            case "text" =>
              sc
                .textFile(inputStr)
                .map(_.getBytes)

            case "sequence" =>
              sc
                .newAPIHadoopFile[BytesWritable, NullWritable,
                  SequenceFileInputFormat[BytesWritable, NullWritable]](inputStr)
                .map(_._1.copyBytes())

            case _ =>
              throw new RuntimeException(s"""unknown format: $format""")

          }
        } else {
          if (allowInputNotExist) {
            sc.makeRDD(Array[Array[Byte]]())
          } else {
            throw new RuntimeException(s"""input Path non exist: $inputStr""")
          }
        }
      })
    sc.union(inputRDDs)
  }

}
