package com.example.kafka_reliability;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import com.example.kafka_reliability.service.EventTrackingService;
import org.springframework.kafka.listener.RetryListener;

@Configuration
public class KafkaConfig {
     @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name("orders.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            EventTrackingService eventTrackingService) {
                DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        "orders.DLT",
                                        record.partition()
                                )
                );
                recoverer.setFailIfSendResultIsError(true);

                ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(3);

                backOff.setInitialInterval(1000L);
                backOff.setMultiplier(2.0);
                backOff.setMaxInterval(4000L);

                ConsumerRecordRecoverer trackingRecoverer = (record, exception) -> {
                    recoverer.accept(record, exception);
                    if (record.key() instanceof String eventId) {
                        eventTrackingService.markDeadLettered(
                                eventId,
                                exception.getMessage() == null
                                        ? exception.toString()
                                        : exception.getMessage()
                        );
                    }
                };
                DefaultErrorHandler errorHandler =
                        new DefaultErrorHandler(trackingRecoverer, backOff);
                errorHandler.setRetryListeners(new RetryListener() {
                    @Override
                    public void failedDelivery(
                            org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                            Exception exception,
                            int deliveryAttempt) {
                        if (record.key() instanceof String eventId) {
                            eventTrackingService.recordAttempt(
                                    eventId,
                                    deliveryAttempt,
                                    exception.getMessage() == null
                                            ? exception.toString()
                                            : exception.getMessage()
                            );
                        }
                    }
                });
                return errorHandler;
        }
}
