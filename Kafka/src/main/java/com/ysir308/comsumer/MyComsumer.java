package com.ysir308.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyComsumer {

    public static void main(String[] args) {

        // 创建配置信息
        Properties prop = new Properties();

        // 给配置信息赋值
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        // 开启自动提交
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交延迟
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // Key，Value的反序列化
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 消费者组
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer(prop);

        // 订阅主题
        consumer.subscribe(Arrays.asList("first", "second"));

        ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

        for (ConsumerRecord<String, String> record : consumerRecords) {
            System.out.println("----------" + record.topic() + "-----------");
            System.out.println(record.key());
            System.out.println(record.value());
            System.out.println(record.offset());
            System.out.println("---------------------");
        }

        consumer.close();
    }
}
