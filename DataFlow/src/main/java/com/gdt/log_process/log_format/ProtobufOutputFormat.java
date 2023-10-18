/**
 * Copyright (c) 2014 Tencent Inc.
 * <p>
 * Author: riccoli@tencent.com Date:   2014-06-13
 */

package com.gdt.log_process.log_format;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

// An InputFormat for protocol buffer(length + record) file.
public class ProtobufOutputFormat extends
    FileOutputFormat<BytesWritable, NullWritable> {

  @Override
  public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(job, ".txt.gz");

    Configuration conf = job.getConfiguration();
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file);

    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);

    return new ProtobufRecordWriter(new DataOutputStream(
        codec.createOutputStream(fileOut)));
  }
}
