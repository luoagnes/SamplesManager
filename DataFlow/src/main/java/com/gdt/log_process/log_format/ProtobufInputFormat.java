/**
 * Copyright (c) 2014 Tencent Inc.
 * <p>
 * Author: riccoli@tencent.com Date:   2014-06-13
 */

package com.gdt.log_process.log_format;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An {@link InputFormat} for protocol buffer(length + record) file.
 */
//@Deprecated
public class ProtobufInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public RecordReader<LongWritable, BytesWritable> createRecordReader(
      InputSplit genericSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new ProtobufRecordReader();
  }
}

