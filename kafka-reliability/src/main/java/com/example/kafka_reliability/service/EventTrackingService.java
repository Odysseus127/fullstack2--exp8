package com.example.kafka_reliability.service;

import com.example.kafka_reliability.OrderEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventTrackingService {

    private final JdbcTemplate jdbcTemplate;

    public EventTrackingService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void markPublishing(OrderEvent event) {
        jdbcTemplate.update(
                """
                INSERT INTO event_tracking (event_id, order_id, status)
                VALUES (?, ?, 'PUBLISHING')
                ON DUPLICATE KEY UPDATE
                    order_id = VALUES(order_id),
                    status = CASE
                        WHEN status IN ('PROCESSED', 'DUPLICATE', 'DEAD_LETTERED')
                            THEN status
                        ELSE 'PUBLISHING'
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                event.getEventId(),
                event.getOrderId()
        );
    }

    public void markPublished(OrderEvent event) {
        jdbcTemplate.update(
                """
                INSERT INTO event_tracking (event_id, order_id, status)
                VALUES (?, ?, 'PUBLISHED')
                ON DUPLICATE KEY UPDATE
                    order_id = VALUES(order_id),
                    status = CASE
                        WHEN status IN ('PROCESSED', 'DUPLICATE', 'DEAD_LETTERED')
                            THEN status
                        ELSE 'PUBLISHED'
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                event.getEventId(),
                event.getOrderId()
        );
    }

    public void markPublishFailed(String eventId, String error) {
        jdbcTemplate.update(
                """
                UPDATE event_tracking
                SET status = CASE
                        WHEN status IN ('PROCESSED', 'DUPLICATE', 'DEAD_LETTERED')
                            THEN status
                        ELSE 'PUBLISH_FAILED'
                    END,
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                """,
                error,
                eventId
        );
    }

    public void markProcessed(OrderEvent event) {
        jdbcTemplate.update(
                """
                INSERT INTO event_tracking (event_id, order_id, status)
                VALUES (?, ?, 'PROCESSED')
                ON DUPLICATE KEY UPDATE
                    order_id = VALUES(order_id),
                    status = 'PROCESSED',
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                """,
                event.getEventId(),
                event.getOrderId()
        );
    }

    public void recordAttempt(String eventId, int attempt, String error) {
        jdbcTemplate.update(
                """
                UPDATE event_tracking
                SET attempt_count = GREATEST(attempt_count, ?),
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                """,
                attempt,
                error,
                eventId
        );
    }

    public void markDeadLettered(String eventId, String error) {
        jdbcTemplate.update(
                """
                UPDATE event_tracking
                SET status = 'DEAD_LETTERED',
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                  AND status NOT IN ('PROCESSED', 'DUPLICATE')
                """,
                error,
                eventId
        );
    }
}
