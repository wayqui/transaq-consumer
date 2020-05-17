package com.wayqui.kafka.consumer;

import com.wayqui.avro.TransactionAvro;
import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.math.BigDecimal;
import java.time.Instant;

@Component
@Slf4j
public class TransactionConsumerManual implements AcknowledgingMessageListener<Integer, TransactionAvro> {

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private KafkaTemplate<Long, TransactionAvro> kafkaTemplate;

    @Override
    @KafkaListener(topics = "transaction-events")
    public void onMessage(ConsumerRecord<Integer, TransactionAvro> consumerRecord, Acknowledgment acknowledgment) {
        log.info("onMessage -> Receiving message with id {}", consumerRecord.value());

        // Insert message to DB
        TransactionAvro transactionMessage = consumerRecord.value();

        Conversions.DecimalConversion DECIMAL_CONVERTER = new Conversions.DecimalConversion();
        LogicalTypes.Decimal decimaltype = LogicalTypes.decimal(6, 2);

        BigDecimal fee = DECIMAL_CONVERTER.fromBytes(transactionMessage.getFee(), null, decimaltype);
        BigDecimal amount = DECIMAL_CONVERTER.fromBytes(transactionMessage.getAmount(), null, decimaltype);

        TransactionDto transactionDto = TransactionDto.builder()
                .reference(consumerRecord.value().getReference())
                .iban(transactionMessage.getIban())
                .fee(fee)
                .amount(amount)
                .date(Instant.ofEpochMilli(transactionMessage.getDate()))
                .description(transactionMessage.getDescription())
                .build();

        transactionService.insertTransaction(transactionDto);

        // Commit offset
        acknowledgment.acknowledge();
        log.info("onMessage -> Message was received correctly");
    }

    public void sendRecoverable(Long key, TransactionAvro value) {
        log.info("sendRecoverable -> Resending recovered message with key {} and value {}", key, value);

        ListenableFuture<SendResult<Long, TransactionAvro>> listenerFuture = kafkaTemplate.sendDefault(key, value);

        listenerFuture.addCallback(new ListenerCallback(key, value));
    }


    private static class ListenerCallback implements ListenableFutureCallback<SendResult<Long, TransactionAvro>> {

        private Long key;
        private TransactionAvro value;

        public ListenerCallback(Long key, TransactionAvro value) {
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
        public void onSuccess(SendResult<Long, TransactionAvro> integerStringSendResult) {
            log.info("This is the message {} ==> {} and the partition is {}", key, value, integerStringSendResult.getRecordMetadata().partition());
        }
    }
}