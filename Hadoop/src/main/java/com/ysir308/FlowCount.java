package com.ysir308;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCount.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置自定义分区规则
        job.setPartitionerClass(ProvincePartitioner.class);
        // 自定义分区规则必须设置NumReduceTasks，默认为1
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}


class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        k.set(fields[1]);   // 手机号

        long upFlow = Long.parseLong(fields[fields.length - 3]);   // 上行流量
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
//        v.setSumFlow(upFlow, downFlow);

        context.write(k, v);
    }
}

class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;

        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }

        FlowBean v = new FlowBean();
        v.set(sum_upFlow, sum_downFlow);

        context.write(key, v);
    }
}

class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 按照手机号前缀分区
     * 将136、137、138、139开头的手机号分别统计到不同的文件中
     */
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // key和value分别是map阶段的输出
        // key是手机号，value是流量信息（FlowBean对象）

        // 获取手机号前三位
        String prePhoneNum = key.toString().substring(0, 3);

        int partition = 4;

        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        }

        return partition;
    }
}