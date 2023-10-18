package com.gdt.log_process.log_format;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

  @Override
  public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    RecordReader<LongWritable, BytesWritable> recordReader = new WholeFileRecordReader();
    recordReader.initialize(split, context);
    return recordReader;
  }
}
