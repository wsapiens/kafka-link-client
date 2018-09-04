package com.all4canid.link.adapter;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdapterApplication {
    private static final Logger log = LoggerFactory.getLogger(AdapterApplication.class);

	public static void main(String[] args) {
	    String groupId = "kafka-link-adapter-consumer";
        String bootStrapServer = "localhost:9092";
        List<String> inputTopics = Arrays.asList("klink-streams-export");
        String outputTopic = "klink-streams-import";

        AdapterProducer producer = new AdapterProducer(bootStrapServer, outputTopic);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(1);
        AdapterConsumer consumer = new AdapterConsumer(bootStrapServer, groupId, inputTopics, producer);
        consumerExecutor.submit(consumer);

		// attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("kafka-link-adapter-shutdown-hook") {
            @Override
            public void run() {
                consumer.shutdown();
                consumerExecutor.shutdown();
                try {
                    consumerExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.warn(e.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
        });
	}
}
