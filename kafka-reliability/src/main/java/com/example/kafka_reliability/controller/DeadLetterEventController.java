package com.example.kafka_reliability.controller;

import com.example.kafka_reliability.dto.PageResponse;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dead-letter-events")
public class DeadLetterEventController {

    private static final int MAX_PAGE_SIZE = 100;
    private final JdbcTemplate jdbcTemplate;

    public DeadLetterEventController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public PageResponse<Map<String, Object>> listDeadLetterEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        if (page < 0 || size < 1 || size > MAX_PAGE_SIZE) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "page must be non-negative and size must be between 1 and 100"
            );
        }

        Long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_tracking WHERE status = 'DEAD_LETTERED'",
                Long.class
        );
        List<Map<String, Object>> deadLetterEvents = jdbcTemplate.queryForList(
                """
                SELECT event_id, order_id, status, attempt_count,
                       last_error, created_at, updated_at
                FROM event_tracking
                WHERE status = 'DEAD_LETTERED'
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?
                """,
                size,
                (long) page * size
        );
        return PageResponse.of(
                deadLetterEvents,
                page,
                size,
                total == null ? 0 : total
        );
    }
}
