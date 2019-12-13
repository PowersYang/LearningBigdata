package com.ysir308.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallBackProducer {

    public static void main(String[] args) {

        Properties prop = new Properties();

        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<String, String>("first", "ysir " + i), new Callback() {

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    // 成功的话返回的是RecordMetadata，失败了返回的是Exception
                    if (e == null) {
                        System.out.println("-----------------------");
                        System.out.println(recordMetadata.topic());
                        System.out.println(recordMetadata.partition());
                        System.out.println(recordMetadata.offset());
                        System.out.println("-----------------------");
                    } else {
                        e.printStackTrace();
                    }
                }
            });

        }

        producer.close();

    }
}
