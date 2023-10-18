/**
 * Copyright (c) 2014 Tencent Inc.
 * <p>
 * Author: riccoli@tencent.com Date:   2014-06-13
 */

package com.gdt.log_process.log_format;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ProtobufRecordWriter extends
    RecordWriter<BytesWritable, NullWritable> {

  protected DataOutputStream out;

  public ProtobufRecordWriter(DataOutputStream out) {
    this.out = out;
  }

  private void writeLittleEndianInt(int data) throws IOException {
    for (int i = 0; i <= 24; i += 8) {
      byte b = (byte) ((data >> i) & 0xFF);
      out.writeByte(b);
    }
  }

  private void writeProtobufObject(BytesWritable key) throws IOException {
    writeLittleEndianInt(key.getLength());
    out.write(key.getBytes(), 0, key.getLength());
  }

  public void write(BytesWritable key, NullWritable value) throws IOException,
      InterruptedException {
    writeProtobufObject(key);
  }

  public void close(TaskAttemptContext arg0) throws IOException,
      InterruptedException {
    out.flush();
    out.close();
  }

}
