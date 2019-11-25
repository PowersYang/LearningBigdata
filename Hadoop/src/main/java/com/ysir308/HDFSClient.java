package com.ysir308;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

    @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {

        // 1、获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "ysir");

        // 2、执行上传
        fs.copyToLocalFile(new Path("/usr/ysir/水电费.xlsx"), new Path("/Users/ysir/temp/水电费.txt"));

        // 3、关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "ysir");

        fs.delete(new Path("/usr/ysir"), true);

        fs.close();
    }

    @Test
    public void testRename() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "ysir");

        fs.rename(new Path("/usr/ysir2"), new Path("/usr/ysir"));

        fs.close();
    }

    @Test
    public void testListFiles()throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "ysir");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/usr/ysir"), true);

        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getBlockSize());
        }


        fs.close();
    }
}
