package com.ysir308.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionProducer {
    public static void main(String[] args) {

        // 创建Kafka生产者的配置信息
        Properties properties = new Properties();

        // 指定连接的集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102：9092");

        // 指定ACK应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);

        // 批次大小  16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // RecordAccumulator缓冲区大小 32M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 指定自定义分区规则
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.ysir308.producer.MyPartitioner");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            // 发送数据
            producer.send(new ProducerRecord<String, String>("first", "ysir " + i));
        }


        // 关闭资源
        producer.close();
    }
}
