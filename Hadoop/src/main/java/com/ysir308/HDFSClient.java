package com.ysir308;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        // 1、获取HDFS客户端对象
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
//        FileSystem fs = FileSystem.get(conf);
//
//        // 2、在HDFS上创建路径
//        fs.mkdirs(new Path("/usr/ysir"));
//
//        // 3、关闭资源
//        fs.close();

        // 1、获取HDFS客户端对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "ysir");

        // 2、在HDFS上创建路径
        fs.mkdirs(new Path("/usr/ysir2"));

        // 3、关闭资源
        fs.close();
    }

    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1、获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "ysir");

        // 2、执行上传
        fs.copyFromLocalFile(new Path("/Users/ysir/Documents/水电费.xlsx"), new Path("/usr/ysir"));

        // 3、关闭资源
        fs.close();

    }
}
