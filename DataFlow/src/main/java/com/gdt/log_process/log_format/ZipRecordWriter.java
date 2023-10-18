/**
 * Copyright (c) 2016 Tencent Inc.
 * <p>
 * Author: huiqinyi@tencent.com Date:   2016-01-27
 */

package com.gdt.log_process.log_format;

import java.io.IOException;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ZipRecordWriter extends
    RecordWriter<BytesWritable, NullWritable> {

  protected ZipOutputStream out;

  public ZipRecordWriter(ZipOutputStream out) {
    this.out = out;
  }

  public void write(BytesWritable key, NullWritable value) throws IOException,
      InterruptedException {
    out.write(key.getBytes(), 0, key.getLength());
  }

  public void close(TaskAttemptContext arg0) throws IOException,
      InterruptedException {
    out.flush();
    out.close();
  }

}
