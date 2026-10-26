package com.example.kafka_reliability.dto;

import java.util.List;

public record PageResponse<T>(
        List<T> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    public static <T> PageResponse<T> of(
            List<T> content,
            int page,
            int size,
            long totalElements) {
        int totalPages = totalElements == 0
                ? 0
                : (int) Math.ceil((double) totalElements / size);
        return new PageResponse<>(content, page, size, totalElements, totalPages);
    }
}
