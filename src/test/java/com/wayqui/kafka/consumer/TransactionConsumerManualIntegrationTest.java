package com.wayqui.kafka.consumer;

import com.wayqui.avro.TransactionAvro;
import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
@EmbeddedKafka(
        topics = {"transaction-events"},
        partitions = 3,
        brokerProperties={
                "log.dir=out/embedded-kafka"
        })
@TestPropertySource(
        properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class TransactionConsumerManualIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Long, TransactionAvro> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    TransactionConsumerManual consumerSpy;

    @SpyBean
    TransactionService serviceSpy;

    @Autowired
    TransactionService service;

    TransactionDto transactionDto;

    @BeforeEach
    void setUp() {
        endpointRegistry.getListenerContainers().forEach(container -> {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        });

        transactionDto = TransactionDto.builder()
                .reference(UUID.randomUUID().toString())
                .amount(BigDecimal.valueOf(13.0))
                .date(Instant.now())
                .description("Restaurant payment")
                .fee(BigDecimal.valueOf(1.18))
                .iban("ES9820385778983000760236")
                .build();
    }

    @Test
    void registerNewTransaction() throws ExecutionException, InterruptedException {
        // Given
        ByteBuffer feeBuff = ByteBuffer.wrap(transactionDto.getFee().unscaledValue().toByteArray());
        ByteBuffer amountBuff = ByteBuffer.wrap(transactionDto.getAmount().unscaledValue().toByteArray());

        TransactionAvro transactionAvro = TransactionAvro.newBuilder()
                .setFee(feeBuff)
                .setAmount(amountBuff)
                .setReference(transactionDto.getReference())
                .setIban(transactionDto.getIban())
                .setDescription(transactionDto.getDescription())
                .setDate(transactionDto.getDate().toEpochMilli())
                .build();

        kafkaTemplate.sendDefault(transactionAvro).get();

        // When
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Then
        verify(consumerSpy, times(1)).onMessage(isA(ConsumerRecord.class), isA(Acknowledgment.class));
        verify(serviceSpy, times(1)).insertTransaction(isA(TransactionDto.class));

        List<TransactionDto> results = service.findAllTransactions();

        assert results.size() > 0;
    }

    @Test
    void throwing_recoverable_error() throws ExecutionException, InterruptedException {
        // Given
        transactionDto.setFee(transactionDto.getAmount().multiply(BigDecimal.valueOf(-1)));

        ByteBuffer feeBuff = ByteBuffer.wrap(transactionDto.getFee().unscaledValue().toByteArray());
        ByteBuffer amountBuff = ByteBuffer.wrap(transactionDto.getAmount().unscaledValue().toByteArray());

        TransactionAvro transactionAvro = TransactionAvro.newBuilder()
                .setFee(feeBuff)
                .setAmount(amountBuff)
                .setReference(UUID.randomUUID().toString())
                .setIban(transactionDto.getIban())
                .setDescription(transactionDto.getDescription())
                .setDate(transactionDto.getDate().toEpochMilli())
                .build();

        kafkaTemplate.sendDefault(transactionAvro).get();

        // When
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Then
        verify(consumerSpy, times(4)).onMessage(isA(ConsumerRecord.class), isA(Acknowledgment.class));
        verify(serviceSpy, times(4)).insertTransaction(isA(TransactionDto.class));

        List<TransactionDto> results = service.findAllTransactions();

        assert results.size() > 0;
    }

    @Test
    void throwing_non_recoverable_error() throws ExecutionException, InterruptedException {
        // Given
        transactionDto.setAmount(transactionDto.getAmount().multiply(new BigDecimal(-1)));

        ByteBuffer feeBuff = ByteBuffer.wrap(transactionDto.getFee().unscaledValue().toByteArray());
        ByteBuffer amountBuff = ByteBuffer.wrap(transactionDto.getAmount().unscaledValue().toByteArray());

        TransactionAvro transactionAvro = TransactionAvro.newBuilder()
                .setFee(feeBuff)
                .setAmount(amountBuff)
                .setReference(transactionDto.getReference())
                .setIban(transactionDto.getIban())
                .setDescription(transactionDto.getDescription())
                .setDate(transactionDto.getDate().toEpochMilli())
                .build();

        kafkaTemplate.sendDefault(transactionAvro).get();

        // When
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Then
        verify(consumerSpy, times(1)).onMessage(isA(ConsumerRecord.class), isA(Acknowledgment.class));
        verify(serviceSpy, times(1)).insertTransaction(isA(TransactionDto.class));

        List<TransactionDto> results = service.findAllTransactions();

        assert results.size() > 0;
    }
}
