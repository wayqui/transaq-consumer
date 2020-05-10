package com.wayqui.kafka.consumer;

import com.google.gson.Gson;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.List;
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
        partitions = 3)
@TestPropertySource(
        properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class TransactionConsumerManualIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    TransactionConsumerManual consumerSpy;

    @SpyBean
    TransactionService serviceSpy;

    @Autowired
    TransactionService service;

    @BeforeEach
    void setUp() {
        endpointRegistry.getListenerContainers().forEach(container -> {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        });
    }

    @Test
    void registerNewTransaction() throws ExecutionException, InterruptedException {
        // Given
        TransactionDto transactionDto = TransactionDto.builder()
                .amount(13.0)
                .date(Instant.now())
                .description("Restaurant payment")
                .fee(1.18)
                .iban("ES9820385778983000760236")
                .build();
        String transactJSON = new Gson().toJson(transactionDto);

        kafkaTemplate.sendDefault(transactJSON).get();

        // When
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Then
        //verify(consumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        //verify(serviceSpy, times(1)).insertTransaction(isA(ConsumerRecord.class));

        List<TransactionDto> results = service.findAllTransactions();

        assert results.size() > 0;
        
    }
}
