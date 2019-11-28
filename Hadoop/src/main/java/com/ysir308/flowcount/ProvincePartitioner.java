package com.ysir308.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

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
