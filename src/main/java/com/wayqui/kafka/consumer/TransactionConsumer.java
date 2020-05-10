package com.wayqui.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class TransactionConsumer {

    //@KafkaListener(topics = "transaction-events")
    public void onMessage(ConsumerRecord<Integer, String> record) {
        log.info(record.toString());
    }
}
