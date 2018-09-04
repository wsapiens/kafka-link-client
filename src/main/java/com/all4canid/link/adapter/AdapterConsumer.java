package com.all4canid.link.adapter;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdapterConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AdapterConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final List<String> inputTopics;
    private AdapterProducer producer;

    public AdapterConsumer(String bootStrapServer, String groupId, List<String> inputTopics, AdapterProducer producer) {
        this.inputTopics = Collections.unmodifiableList(inputTopics);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        this.producer = producer;
    }



    @Override
    public void run() {
        try {
            consumer.subscribe(inputTopics);

            while (!Thread.currentThread().isInterrupted()) {
                final ConsumerRecords<String, String> records = consumer.poll(1000);
                if(records.isEmpty()) {
                   continue; 
                }
                records.forEach(record -> {
                    log.info("Consumer Record:({}, {}, {}, {})", record.key(), record.value(), record.partition(), record.offset());
                    if(StringUtils.isNotBlank(record.value())) {
                        try {
                            log.info(record.value());
                            //TODO work by actionType from input message
                            String contractData = "{\"SubContract\":[{\"MainJobNumber\":\"17500\",\"SubJobNumber\":null,\"VendorId\":\"SPIDER\",\"ContractNumber\":\"MPC100059\",\"ContractDescription\":\"Spider, divison of SafeWorks\",\"ContractDate\":\"04202018\",\"DefaultRetentionPercent\":6,\"DiscountPercent\":null,\"SubcontractAmount\":1230001,\"DiscountWindow\":0,\"DoNotExceed\":null,\"Components\":[{\"MainJobNumber\":\"17500\",\"SubcontractItemNumber\":\"1.1.1\",\"ComponentDescription\":\"test\",\"AccountCode\":\"100.HQ.501020.00000.000.0000\",\"CategoryCode\":\"Labor\",\"SubJobNumber\":\"300000002266341\",\"TaxCode\":null,\"VendorId\":null,\"ContractNumber\":null,\"UnitOfMeasure\":\"EA\",\"TaskSequence\":null,\"UnitPrice\":1,\"ComponentAmount\":1000001,\"ComponentRetentionPercent\":6,\"UnitQuantity\":1000001,\"ComponentType\":null,\"PhaseCode\":null}]}]}";
                            producer.send(contractData);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    }
                });
                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
