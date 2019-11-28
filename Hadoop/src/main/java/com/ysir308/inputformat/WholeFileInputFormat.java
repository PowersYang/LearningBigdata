package com.ysir308.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义InputFormat案例
 */

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        WholeRecoderReader recoderReader = new WholeRecoderReader();
        recoderReader.initialize(inputSplit, taskAttemptContext);


        return null;
    }
}
