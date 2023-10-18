package com.gdt.log_process.log_format;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class WholeFileCombineRecordReader extends RecordReader<LongWritable, BytesWritable> {

  private static final Log log = LogFactory.getLog(WholeFileCombineRecordReader.class);
  private JobContext jobContext;
  private LongWritable currentKey = new LongWritable(0);
  private BytesWritable currentValue;
  private boolean finishConverting = false;
  private boolean readError = false;
  private int len;
  private Path file;

  public WholeFileCombineRecordReader(CombineFileSplit split, TaskAttemptContext context,
      Integer index) throws IOException {
    len = (int) split.getLength(index);
    file = split.getPath(index);
    this.jobContext = context;
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {

  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return currentKey;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (len == 0) {
      log.info("empty file : " + file.getName());
      return false;
    }
    if (!finishConverting) {
      currentValue = new BytesWritable();
      byte[] content = new byte[len];
      FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, content, 0, len);
        currentValue.set(content, 0, len);
      } catch (NullPointerException ex) {
        log.error(ex.getMessage());
        readError = true;
      } finally {
        if (in != null) {
          IOUtils.closeStream(in);
        }
      }
      if (readError) {
        return false;
      }
      finishConverting = true;
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() throws IOException {
    float progress = 0;
    if (finishConverting) {
      progress = 1;
    }
    return progress;
  }

  @Override
  public void close() throws IOException {
  }
}
