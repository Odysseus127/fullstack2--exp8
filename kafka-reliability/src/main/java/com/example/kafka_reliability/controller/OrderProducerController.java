package com.example.kafka_reliability.controller;

import com.example.kafka_reliability.OrderEvent;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.example.kafka_reliability.service.EventTrackingService;

@RestController
@RequestMapping("/api/orders")
public class OrderProducerController {
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final EventTrackingService eventTrackingService;

    public OrderProducerController(
            KafkaTemplate<Object, Object> kafkaTemplate,
            EventTrackingService eventTrackingService) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventTrackingService = eventTrackingService;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<String>> publish(
            @RequestBody OrderEvent event) {

        if (event == null) {
            return CompletableFuture.completedFuture(
                    ResponseEntity.badRequest().body("Order event is required")
            );
        }

        if (event.getOrderId() == null || event.getOrderId().isBlank()) {
            return CompletableFuture.completedFuture(
                    ResponseEntity.badRequest().body("Order ID is required")
            );
        }

        if (event.getStatus() == null || event.getStatus().isBlank()) {
            return CompletableFuture.completedFuture(
                    ResponseEntity.badRequest().body("Order status is required")
            );
        }

        if (event.getEventId() == null
                || event.getEventId().isBlank()) {
            event.setEventId(UUID.randomUUID().toString());
        }
        eventTrackingService.markPublishing(event);

        return kafkaTemplate.send(
                        "orders",
                        event.getEventId(),
                        event
                )
                .handle((result, error) -> {
                    if (error != null) {
                        String message = error.getMessage() == null
                                ? error.toString()
                                : error.getMessage();

                        eventTrackingService.markPublishFailed(
                                event.getEventId(),
                                message
                        );

                        return ResponseEntity
                                .status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body("Could not confirm event publication. ID: "
                                        + event.getEventId());
                        }
                        eventTrackingService.markPublished(event);
                        return ResponseEntity
                                .accepted()
                                .body("Event published. ID: "
                                        + event.getEventId());
                    });
        }
}
