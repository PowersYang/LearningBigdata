package com.ysir308.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 从HBase的A表读数据至B表
 */
public class FruitDriver2 implements Tool {

    // 配置信息
    private Configuration configuration = null;

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(configuration);

        job.setJarByClass(FruitDriver2.class);

        // 设置Mapper相关信息
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class, job);

        // 设置Reducer输出信息
        TableMapReduceUtil.initTableReducerJob("fruit2", Fruit2Reducer.class, job);

        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {

        Configuration conf = new Configuration();

        try {

            ToolRunner.run(conf, new FruitDriver2(), args);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 构建put对象
        Put put = new Put(key.get());

        // 获取数据
        for (Cell cell : value.rawCells()) {
            // 判断当前的cell是否为name列
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {

                put.add(cell);

            }
        }

        context.write(key, put);

    }
}

class Fruit2Reducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        for (Put put : values) {

            context.write(NullWritable.get(), put);

        }

    }
}