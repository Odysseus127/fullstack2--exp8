package com.example.kafka_reliability.consumer;

import com.example.kafka_reliability.OrderEvent;
import com.example.kafka_reliability.service.OrderProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    private final OrderProcessingService processingService;

    public OrderConsumer(
            OrderProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(
            topics = "orders",
            groupId = "order-group"
    )
    public void consume(OrderEvent event) {

        System.out.println(
                "Received event: " + event.getEventId());

        String result = processingService.processOrder(event);

        if ("DUPLICATE".equals(result)) {
            System.out.println(
                    "Duplicate ignored: " + event.getEventId());
            return;
        }

        System.out.println(
                "Order processed successfully: "
                        + event.getOrderId());
    }
}
