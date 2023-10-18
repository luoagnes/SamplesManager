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

public class WholeFileCombineInputFormat extends
    CombineFileInputFormat<LongWritable, BytesWritable> {

  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public RecordReader<LongWritable, BytesWritable> createRecordReader(
      InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable, BytesWritable>(
        (CombineFileSplit) genericSplit, context, WholeFileCombineRecordReader.class);
  }
}
