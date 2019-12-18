package com.ysir308.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 在传入的数据中添加时间戳
 */
public class TimeInterceptor implements ProducerInterceptor {


    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        long timeStamp = System.currentTimeMillis();

        String value = producerRecord.value().toString();

        ProducerRecord newRecord = new ProducerRecord(producerRecord.topic(), producerRecord.partition(), timeStamp + "," + value);
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

}
