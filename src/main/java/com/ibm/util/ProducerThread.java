package com.ibm.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerThread implements Runnable{
    private KafkaProducer<String, String> producer = null;
    private ProducerRecord<String, String> record = null;

    public ProducerThread(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        this.producer = producer;
        this.record = record;
    }

    @Override
    public void run() {
        producer.send(record);
    }
}
