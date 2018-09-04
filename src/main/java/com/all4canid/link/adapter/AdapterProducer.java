package com.all4canid.link.adapter;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdapterProducer  {
    private static final Logger log = LoggerFactory.getLogger(AdapterProducer.class);

    private ReadWriteLock rwlock = new ReentrantReadWriteLock();

    private String topic;
    private KafkaProducer<String, String> producer;

    public AdapterProducer(String bootStrapServer, String topic) {
        //TODO make serde proper for domain object, we might want to make producer for each import api
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, topic + "-adapter-client");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    public void send(final String data) {
        rwlock.writeLock().lock();
        try {
            if(StringUtils.isNotBlank(data)) {
                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, data)).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
        } finally {
            rwlock.writeLock().unlock();
        }
    }

}
