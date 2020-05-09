package com.wayqui.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
@EmbeddedKafka(
        topics = {"transaction-events"},
        partitions = 3,
        brokerProperties={
                "log.dir=out/embedded-kafka"
        })
@TestPropertySource(
        properties = {
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
public class TransactionConsumerManualIntegrationTest {




}
