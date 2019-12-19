package com.ysir308.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 读取本地文件，上传至HBase
 */
public class FruitDriver implements Tool {

    // 定义Configuration
    Configuration conf = new Configuration();

    public int run(String[] args) throws Exception {

        // 获取Job方法
        Job job = Job.getInstance(conf);

        // 设置驱动类路径
        job.setJarByClass(FruitDriver.class);

        // 设置Mapper以及Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit", FruitReducer.class, job);

        // 设置最终输出数据的KV类型
        // 不用写


        // 设置输入输出参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 提交任务
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {


        try {
            Configuration conf = new Configuration();
            int run = ToolRunner.run(conf, new FruitDriver(), args);

            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


class FruitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.write(key, value);
    }
}

class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String[] fields = value.toString().split("\t");

            // 构建put对象
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));

            context.write(NullWritable.get(), put);
        }
    }
}