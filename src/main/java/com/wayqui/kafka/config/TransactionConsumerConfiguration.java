package com.wayqui.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
@EnableKafka
@Slf4j
public class TransactionConsumerConfiguration {

    @Bean
    @ConditionalOnMissingBean(
            name = {"kafkaListenerContainerFactory"}
    )
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable());

        // Configuring manual commit of the offsets
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Configuring error handler
        factory.setErrorHandler((e, consumerRecord) -> {
            log.error("An exception occurred {}, the message is {}", e.getMessage(), consumerRecord.toString());
        });

        // Configure custom retry policy
        factory.setRetryTemplate(createRetryTemplate(3, 1000));

        return factory;
    }

    /**
     * Create a new retry policy based on max number of attempts and backOffPeriod
     * @return Customized RetryPolicy
     */
    private RetryTemplate createRetryTemplate(int maxAttempts, long backOffPeriod) {
        RetryTemplate retryTemplate = new RetryTemplate();
        FixedBackOffPolicy fixedPolicy = new FixedBackOffPolicy();
        fixedPolicy.setBackOffPeriod(backOffPeriod);
        retryTemplate.setBackOffPolicy(fixedPolicy);
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(maxAttempts));
        return retryTemplate;
    }
}
