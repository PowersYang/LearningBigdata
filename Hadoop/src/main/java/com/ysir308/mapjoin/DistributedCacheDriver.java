package com.ysir308.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class DistributedCacheDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DistributedCacheDriver.class);

        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI("/Users/ysir/pd.txt"));

        // 不需要reduce阶段
        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    HashMap<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        // 缓存小表格
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }

        IOUtils.closeStream(reader);
    }

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        // 取出pname
        String pid = fields[1];
        String pname = pdMap.get(pid);

        // 拼接
        line = line + "\t" + pname;
        k.set(line);

        context.write(k, NullWritable.get());
    }
}