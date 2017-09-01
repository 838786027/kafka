package com.cpphot.action;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public Consumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cxp1:9092,cxp2:9092,cxp3:9092");
        props.put("zookeeper.connect", "cxp1:2181,cxp2:2181,cxp3:2181");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//latest,earliest
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    public void consumerMsg(){
        try {
            consumer.subscribe(Collections.singletonList(this.topic));
            while(true){
                ConsumerRecords<Integer, String> records = consumer.poll(1000);
                for (ConsumerRecord<Integer, String> record : records) {
                    System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at partition "+record.partition()+" offset " + record.offset());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Consumer Consumer = new Consumer(Constants.TOPIC);
        Consumer.consumerMsg();
    }

}
