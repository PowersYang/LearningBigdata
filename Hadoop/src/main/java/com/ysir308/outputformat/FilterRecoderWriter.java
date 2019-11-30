package com.ysir308.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecoderWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream fosYisr;
    FSDataOutputStream fosOther;

    public FilterRecoderWriter(TaskAttemptContext taskAttemptContext) {
        try {
            // 获取文件系统
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());

            // 创建输出到ysir.log的输出流
            fosYisr = fs.create(new Path("/Users/ysir/ysir.log"));

            // 创建输出到other.log的输出路
            fosOther = fs.create(new Path("/Users/ysir/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        // 判断key中是否包含ysir
        if (text.toString().contains("ysir")) {
            fosYisr.write(text.toString().getBytes());
        } else {
            fosOther.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(fosYisr);
        IOUtils.closeStream(fosOther);

    }
}
