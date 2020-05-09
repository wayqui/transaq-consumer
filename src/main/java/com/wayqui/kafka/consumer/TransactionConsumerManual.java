package com.wayqui.kafka.consumer;

import com.google.gson.Gson;
import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TransactionConsumerManual implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    private TransactionService transactionService;

    @Override
    @KafkaListener(topics = "transaction-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Receiving message with id {}", consumerRecord.value());

        // Insert message to DB
        TransactionDto transactionDto = new Gson().fromJson(consumerRecord.value(), TransactionDto.class);
        transactionService.insertTransaction(transactionDto);

        // Commit offset
        acknowledgment.acknowledge();
    }
}
