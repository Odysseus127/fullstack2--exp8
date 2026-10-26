package com.example.kafka_reliability;

import com.example.kafka_reliability.controller.EventTrackingController;
import com.example.kafka_reliability.dto.PageResponse;
import com.example.kafka_reliability.service.EventTrackingService;
import com.example.kafka_reliability.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BackendBehaviorTests {

    @Test
    void duplicateEventIsNotInsertedAgain() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EventTrackingService trackingService = mock(EventTrackingService.class);
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(0);

        OrderProcessingService service =
                new OrderProcessingService(jdbcTemplate, trackingService);

        String result = service.processOrder(
                new OrderEvent("event-1", "order-1", "CREATED", false)
        );

        assertEquals("DUPLICATE", result);
        verify(jdbcTemplate, times(1))
                .update(anyString(), any(Object[].class));
        verifyNoInteractions(trackingService);
    }

    @Test
    void successfulEventWritesOrderAndMarksItProcessed() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EventTrackingService trackingService = mock(EventTrackingService.class);
        when(jdbcTemplate.update(anyString(), any(Object[].class)))
                .thenReturn(1, 1);
        OrderEvent event = new OrderEvent("event-2", "order-2", "CREATED", false);

        OrderProcessingService service =
                new OrderProcessingService(jdbcTemplate, trackingService);

        assertEquals("PROCESSED", service.processOrder(event));
        verify(jdbcTemplate, times(2))
                .update(anyString(), any(Object[].class));
        verify(trackingService).markProcessed(event);
    }

    @Test
    void simulatedFailureDoesNotInsertOrderOrMarkEventProcessed() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EventTrackingService trackingService = mock(EventTrackingService.class);
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        OrderEvent event = new OrderEvent("event-3", "order-3", "CREATED", true);
        OrderProcessingService service =
                new OrderProcessingService(jdbcTemplate, trackingService);

        assertThrows(RuntimeException.class, () -> service.processOrder(event));

        verify(jdbcTemplate, times(1))
                .update(anyString(), any(Object[].class));
        verifyNoInteractions(trackingService);
    }

    @Test
    void eventListReturnsPaginationMetadata() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.queryForObject(
                anyString(),
                eq(Long.class),
                any(Object[].class)
        )).thenReturn(25L);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of());
        EventTrackingController controller =
                new EventTrackingController(jdbcTemplate);

        PageResponse<Map<String, Object>> result =
                controller.listEvents(1, 10, " processed ");

        assertEquals(1, result.page());
        assertEquals(10, result.size());
        assertEquals(25L, result.totalElements());
        assertEquals(3, result.totalPages());
        assertEquals(List.of(), result.content());
    }

    @Test
    void eventListRejectsInvalidPagination() {
        EventTrackingController controller =
                new EventTrackingController(mock(JdbcTemplate.class));

        ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> controller.listEvents(0, 101, null)
        );

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
    }
}
