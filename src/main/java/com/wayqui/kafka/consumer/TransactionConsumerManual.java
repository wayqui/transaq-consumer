package com.wayqui.kafka.consumer;

import com.google.gson.Gson;
import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.UUID;

@Component
@Slf4j
public class TransactionConsumerManual implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private KafkaTemplate<Long, String> kafkaTemplate;

    @Override
    @KafkaListener(topics = "transaction-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("onMessage -> Receiving message with id {}", consumerRecord.value());

        // Insert message to DB
        TransactionDto transactionDto = new Gson().fromJson(consumerRecord.value(), TransactionDto.class);

        transactionService.insertTransaction(transactionDto);

        // Commit offset
        acknowledgment.acknowledge();
        log.info("onMessage -> Message was received correctly");
    }

    public void sendRecoverable(Long key, String value) {
        log.info("sendRecoverable -> Resending recovered message with key {} and value {}", key, value);
        TransactionDto transactionDto = new Gson().fromJson(value, TransactionDto.class);

        // Fixing the message in order to solve the recoverable error
        if (transactionDto.getReference() == null) transactionDto.setReference(UUID.randomUUID().toString());

        String transactJSON = new Gson().toJson(transactionDto);
        ListenableFuture<SendResult<Long, String>> listenerFuture = kafkaTemplate.sendDefault(key, transactJSON);

        listenerFuture.addCallback(new ListenerCallback(key, transactJSON));

    }


    private static class ListenerCallback implements ListenableFutureCallback<SendResult<Long, String>> {

        private Long key;
        private String value;

        public ListenerCallback(Long key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void onFailure(Throwable throwable) {
            log.error("Error sending message {}", throwable.getMessage());
            try {
                throw throwable;
            } catch (Throwable e) {
                log.error("Error in onFailure {}", e.getMessage());
            }
        }

        @Override
        public void onSuccess(SendResult<Long, String> integerStringSendResult) {
            log.info("This is the message {} ==> {} and the partition is {}", key, value, integerStringSendResult.getRecordMetadata().partition());
        }
    }
}