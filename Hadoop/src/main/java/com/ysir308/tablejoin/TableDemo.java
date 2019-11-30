package com.ysir308.tablejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableDemo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(TableDemo.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        // 输入的文件名称
        name = inputSplit.getPath().getName();
    }

    TableBean bean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (name.startsWith("order")) {
            // 订单表
            String[] fields = line.split(" ");

            bean.setId(fields[0]);
            bean.setPid(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("order");

            k.set(fields[1]);
        } else {
            // 产品表
            String[] fields = line.split(" ");

            bean.setId("");
            bean.setPid(fields[0]);
            bean.setAmount(0);
            bean.setPname(fields[1]);
            bean.setFlag("pd");

            k.set(fields[0]);
        }

        context.write(k, bean);

    }
}


class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        // 存放所有订单集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean tableBean : values) {
            if ("order".equals(tableBean.getFlag())) {
                TableBean tempBean = new TableBean();
                try {
                    // tableBean中记录的只是引用，并不是完整的对象
                    // 如果需要完整的对象，就必须通过这种方式来拷贝
                    BeanUtils.copyProperties(tempBean, tableBean);
                    orderBeans.add(tempBean);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean tableBean : orderBeans) {
            tableBean.setPname(pdBean.getPname());

            context.write(tableBean, NullWritable.get());
        }
    }
}