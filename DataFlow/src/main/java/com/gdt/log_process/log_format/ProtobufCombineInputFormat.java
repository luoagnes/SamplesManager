/**
 * Copyright (c) 2014 Tencent Inc. Author: huiqinyi@tencent.com Date: 2014-07-29 Info:
 * ProtobufCombineInputFormat which returns ProtobufCombineRecordReader combine small files in a
 * mapper
 */

package com.gdt.log_process.log_format;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
* An {@link } for protocol buffer(length + record) file.
 */
//@Deprecated
public class ProtobufCombineInputFormat extends CombineFileInputFormat<LongWritable,
    BytesWritable> {

  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public RecordReader<LongWritable, BytesWritable> createRecordReader(
      InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable, BytesWritable>(
        (CombineFileSplit) genericSplit, context, ProtobufRecordReader.class);
  }
}

