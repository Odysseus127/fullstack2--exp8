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
@RequestMapping("/api/orders")
public class OrderQueryController {

    private static final int MAX_PAGE_SIZE = 100;
    private final JdbcTemplate jdbcTemplate;

    public OrderQueryController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public PageResponse<Map<String, Object>> listOrders(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String status) {
        if (page < 0 || size < 1 || size > MAX_PAGE_SIZE) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "page must be non-negative and size must be between 1 and 100"
            );
        }

        String normalizedStatus = status == null || status.isBlank()
                ? null
                : status.trim();
        String filter = normalizedStatus == null ? "" : " WHERE status = ?";
        Object[] filterArgs = normalizedStatus == null
                ? new Object[0]
                : new Object[]{normalizedStatus};

        Long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM orders" + filter,
                Long.class,
                filterArgs
        );
        List<Object> queryArgs = new java.util.ArrayList<>(
                java.util.Arrays.asList(filterArgs));
        queryArgs.add(size);
        queryArgs.add((long) page * size);

        List<Map<String, Object>> orders = jdbcTemplate.queryForList(
                """
                SELECT order_id, event_id, status, created_at
                FROM orders
                """
                        + filter
                        + " ORDER BY created_at DESC LIMIT ? OFFSET ?",
                queryArgs.toArray()
        );

        return PageResponse.of(orders, page, size, total == null ? 0 : total);
    }
}
