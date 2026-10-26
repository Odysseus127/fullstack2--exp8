package com.example.kafka_reliability.controller;

import com.example.kafka_reliability.dto.PageResponse;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/events")
public class EventTrackingController {

    private static final int MAX_PAGE_SIZE = 100;
    private final JdbcTemplate jdbcTemplate;

    public EventTrackingController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public PageResponse<Map<String, Object>> listEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String status) {
        validatePage(page, size);

        String normalizedStatus = normalizeStatus(status);
        String filter = normalizedStatus == null ? "" : " WHERE status = ?";
        Object[] filterArgs = normalizedStatus == null
                ? new Object[0]
                : new Object[]{normalizedStatus};

        Long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_tracking" + filter,
                Long.class,
                filterArgs
        );
        List<Object> queryArgs = new java.util.ArrayList<>(
                java.util.Arrays.asList(filterArgs));
        queryArgs.add(size);
        queryArgs.add((long) page * size);

        List<Map<String, Object>> events = jdbcTemplate.queryForList(
                """
                SELECT event_id, order_id, status, attempt_count,
                       last_error, created_at, updated_at
                FROM event_tracking
                """
                        + filter
                        + " ORDER BY created_at DESC LIMIT ? OFFSET ?",
                queryArgs.toArray()
        );

        return PageResponse.of(events, page, size, total == null ? 0 : total);
    }

    @GetMapping("/{eventId}")
    public Map<String, Object> getEvent(@PathVariable String eventId) {
        List<Map<String, Object>> events = jdbcTemplate.queryForList(
                """
                SELECT event_id, order_id, status, attempt_count,
                       last_error, created_at, updated_at
                FROM event_tracking
                WHERE event_id = ?
                """,
                eventId
        );
        if (events.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Event not found");
        }
        return events.get(0);
    }

    private static void validatePage(int page, int size) {
        if (page < 0 || size < 1 || size > MAX_PAGE_SIZE) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "page must be non-negative and size must be between 1 and 100"
            );
        }
    }

    private static String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        return status.trim().toUpperCase(java.util.Locale.ROOT);
    }
}
