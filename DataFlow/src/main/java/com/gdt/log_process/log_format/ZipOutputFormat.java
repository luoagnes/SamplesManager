/**
 * Copyright (c) 2016 Tencent Inc.
 * <p>
 * Author: huiqinyi@tencent.com Date:   2016-01-27
 */

package com.gdt.log_process.log_format;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ZipOutputFormat extends
    FileOutputFormat<BytesWritable, NullWritable> {

  private static final Log log = LogFactory.getLog(ZipOutputFormat.class);

  @Override
  public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    Path oriFile = getDefaultWorkFile(job, ".txt");
    Path zipFile = getDefaultWorkFile(job, ".txt.zip");

    Configuration conf = job.getConfiguration();
    FileSystem fs = zipFile.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(zipFile);
    ZipOutputStream zipOut = new ZipOutputStream(fileOut);
    zipOut.putNextEntry(new ZipEntry(oriFile.getName()));
    zipOut.setComment("jd datapass zip data");
    return new ZipRecordWriter(zipOut);
  }
}
