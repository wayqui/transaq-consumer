package com.wayqui.kafka.config;

import com.wayqui.avro.TransactionAvro;
import com.wayqui.kafka.consumer.TransactionConsumerManual;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class TransactionConsumerConfiguration {

    @Autowired
    TransactionConsumerManual consumerManual;

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
            log.error("MANUAL CONSUMER CONFIG: An exception occurred {}", e.getMessage());
        });

        // Configure custom retry policy
        factory.setRetryTemplate(createRetryTemplate(3, 1000));

        // Configure recovery callback (sending the message via de producer to the topic)
        factory.setRecoveryCallback(retryContext -> {
            if (retryContext.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                log.error("Inside the recoverable callback");

                ConsumerRecord<Long, TransactionAvro> record =
                        (ConsumerRecord<Long, TransactionAvro>) retryContext.getAttribute("record");

                consumerManual.sendRecoverable(record.key(), record.value());
            } else {
                log.error("Inside the NON recoverable callback", retryContext.getLastThrowable());
            }

            return null;
        });

        return factory;
    }

    /**
     * Create a new retry policy based on max number of attempts and backOffPeriod
     * @return Customized RetryPolicy
     */
    private RetryTemplate createRetryTemplate(int maxAttempts, long backOffPeriod) {
        RetryTemplate retryTemplate = new RetryTemplate();

        Map<Class<? extends Throwable>, Boolean> exceptions = new HashMap<>();
        exceptions.put(IllegalArgumentException.class, false);
        exceptions.put(RecoverableDataAccessException.class, true);

        FixedBackOffPolicy fixedPolicy = new FixedBackOffPolicy();
        fixedPolicy.setBackOffPeriod(backOffPeriod);

        retryTemplate.setBackOffPolicy(fixedPolicy);
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy((maxAttempts), exceptions, true));
        return retryTemplate;
    }
}
