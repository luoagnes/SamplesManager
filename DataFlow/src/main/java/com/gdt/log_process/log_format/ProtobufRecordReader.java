/**
 * Copyright (c) 2014 Tencent Inc.
 * <p>
 * Author: riccoli@tencent.com Date:   2014-06-13
 * <p>
 * Modified: huiqinyi@tencent.com Date:   2014-07-31 Info:   Support for CombineRecordReader
 */

package com.gdt.log_process.log_format;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Treats keys as offset in file and value as protobuf.
 */

public class ProtobufRecordReader extends RecordReader<LongWritable, BytesWritable> {

  private static final Log log = LogFactory.getLog(ProtobufRecordReader.class);

  public static final String SKIP_BAD_PB_FORMAT =
      "gdt.log_process.com.gdt.log_process.log_format.log_format.ProtobufRecordReader.skip_bad_pb_format";
  public static final String SKIP_READ_EXCEPTION =
      "gdt.log_process.com.gdt.log_process.log_format.log_format.ProtobufRecordReader.skip_read_exception";

  private CompressionCodecFactory compressionCodecs = null;

  private long start;
  private long end;
  private long splitLength;
  private long pos;
  private InputStream in;
  protected Configuration conf;
  private byte[] buffer = new byte[4096];
  private Path file;
  private LongWritable key = null;
  private BytesWritable value = null;
  private boolean initialized = false;
  private boolean skipBadPbFormat = false; // default: throw when meet record with bad format
  private boolean skipReadException = false; // default: throw when meet read exception

  public ProtobufRecordReader() {
  }

  public ProtobufRecordReader(CombineFileSplit split, TaskAttemptContext context,
      Integer index) throws IOException {
    initialized = true;
    start = split.getOffset(index);
    pos = start;
    splitLength = split.getLength(index);
    end = start + splitLength;

    file = split.getPath(index);
    log.info("Split file location:" + file.toUri().toString());
    log.info("Split file start:" + split.getOffset(index));
    log.info("Split file length:" + split.getLength(index));

    initConfig(context.getConfiguration(), split.getPath(index));
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    if (initialized) {
      return;
    }
    FileSplit split = (FileSplit) genericSplit;
    start = split.getStart();
    pos = start;
    splitLength = split.getLength();
    end = start + splitLength;

    file = split.getPath();
    log.info("Split file location:" + file.toUri().toString());
    log.info("Split file start:" + split.getStart());
    log.info("Split file length:" + split.getLength());

    initConfig(context.getConfiguration(), split.getPath());
  }

  private void initConfig(Configuration conf, Path path) throws IOException {
    // open the file and seek to the start of the split
    this.conf = conf;
    FileSystem fs = file.getFileSystem(conf);
    in = fs.open(path);
    compressionCodecs = new CompressionCodecFactory(conf);
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec != null) {
      log.info("codec = " + codec);
      in = codec.createInputStream(in);
      end = Long.MAX_VALUE;
    }

    this.skipBadPbFormat = conf.getBoolean(SKIP_BAD_PB_FORMAT, skipBadPbFormat);
    this.skipReadException = conf.getBoolean(SKIP_READ_EXCEPTION, skipReadException);
    log.info("skip bad pb format: " + skipBadPbFormat);
    log.info("skip read exception: " + skipReadException);
  }

  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  /**
   * Returns the current position in the input.
   *
   * @return the current position in the input.
   */
  public long getPos() {
    return pos;
  }

  // get the record len of current record
  private int readLittleEndianInt(InputStream din) throws IOException, BadPbFormatException {
    int recordLen = 0;
    for (int i = 0; i < 4; ++i) {
      int byteData = din.read();
      if (byteData == -1) {
        if (i == 0) {
          return 0; // end of file
        }
        throw new BadPbFormatException("Parse pb file failed, file=" + file.toUri().toString()
            + ", i=" + i);
      }
      recordLen += (byteData & 0xFF) << (i * 8);
    }
    return recordLen;
  }

  public boolean extendBuffer(int size) {
    while (buffer.length < size) {
      // size过大
      if (buffer.length * 2 <= 0) {
        return false;
      } else {
        buffer = new byte[buffer.length * 2];
      }
    }
    return true;
  }

  /**
   * Read key/value pair in a line.
   */
  public synchronized boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new BytesWritable();
    }
    int size = 0;

    try {
      size = readLittleEndianInt(in);
    } catch (IOException e) {
      return returnOnBadRecord(skipReadException, e, "IOException occurs in readLittleEndianInt()",
          false, SKIP_READ_EXCEPTION);
    } catch (BadPbFormatException e) {
      return returnOnBadRecord(skipBadPbFormat, e, "bad pb format occurs in readLittleEndianInt()",
          false, SKIP_BAD_PB_FORMAT);
    }

    if (size == 0) {
      log.info("read the pb file completely");
      return false;
    }

    if (size < 0) {
      String msg = "Parse pb file error: " + file.toUri().toString() + ", get size " + size;
      return returnOnBadRecord(skipBadPbFormat, new BadPbFormatException(msg),
          "bad pb format in reading size", false, SKIP_BAD_PB_FORMAT);
    }

    pos += 4;
    pos += size;
    key.set(pos);

    int readlen = 0;
    if (size > buffer.length) {
      if (!extendBuffer(size)) {
        return false;
      }
    }
    int already_read = 0;
    while (already_read < size) {
      try {
        readlen = in.read(buffer, already_read, size - already_read);
      } catch (IOException e) {
        return returnOnBadRecord(skipReadException, e, "IOException occurs in read file",
            false, SKIP_READ_EXCEPTION);
      }
      if (readlen == -1) {
        if (already_read < size) {
          String msg = "Parse pb file error: " + file.toUri().toString() + ", current read size "
              + readlen + " but expected size: " + size;
          return returnOnBadRecord(skipBadPbFormat, new BadPbFormatException(msg),
              "bad pb format in already read size", false, SKIP_BAD_PB_FORMAT);
        } else {
          break;
        }
      }
      already_read += readlen;
    }
    value.set(buffer, 0, size);
    return true;
  }

  /**
   * Get the progress within the split.
   */

  //@Override
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  /**
   * if skip, then warn and return returnOnSkip. otherwise, throw new IOE and tell which key to set
   * to skip.
   */
  private boolean returnOnBadRecord(boolean skipException, Exception e, String msg,
      boolean returnOnSkip, String confKey) throws IOException {
    if (skipException) {
      log.warn(msg, e);
      return returnOnSkip;
    } else {
      throw new IOException(msg + ", set " + confKey + " to true to skip", e);
    }
  }

  @SuppressWarnings("serial")
  private class BadPbFormatException extends Exception {

    public BadPbFormatException(String msg) {
      super(msg);
    }
  }
}
