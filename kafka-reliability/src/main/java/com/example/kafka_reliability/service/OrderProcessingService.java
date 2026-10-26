package com.example.kafka_reliability.service;

import com.example.kafka_reliability.OrderEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
//import com.example.kafka_reliability.service.EventTrackingService;

@Service
public class OrderProcessingService {
    private final JdbcTemplate jdbcTemplate;
    private final EventTrackingService eventTrackingService;

    public OrderProcessingService(
           JdbcTemplate jdbcTemplate, 
           EventTrackingService eventTrackingService) {
        this.jdbcTemplate = jdbcTemplate;
        this.eventTrackingService = eventTrackingService;
    }

    @Transactional
    public String processOrder(OrderEvent event) {

        if (event.getEventId() == null
                || event.getEventId().isBlank()) {
            throw new IllegalArgumentException(
                    "Event ID is required");
        }

        if (event.getOrderId() == null
                || event.getOrderId().isBlank()) {
            throw new IllegalArgumentException(
                    "Order ID is required");
        }

        if (event.getStatus() == null
                || event.getStatus().isBlank()) {
            throw new IllegalArgumentException(
                    "Order status is required");
        }

        // Atomically claim this event ID.
        // A previously processed event produces zero inserted rows.
        int inserted = jdbcTemplate.update(
                """
                INSERT IGNORE INTO processed_events
                    (event_id, processed_at)
                VALUES (?, CURRENT_TIMESTAMP)
                """,
                event.getEventId()
        );

        if (inserted == 0) {
            return "DUPLICATE";
        }

        // Simulate a processing failure to test retry and rollback.
        if (event.isShouldFail()) {
            throw new RuntimeException(
                    "Simulated order processing failure");
        }

        // Perform the business operation in the same transaction.
        jdbcTemplate.update(
                """
                INSERT INTO orders (order_id, event_id, status)
                VALUES (?, ?, ?)
                """,
                event.getOrderId(),
                event.getEventId(),
                event.getStatus()
        );
        eventTrackingService.markProcessed(event);

        return "PROCESSED";
    }
}
