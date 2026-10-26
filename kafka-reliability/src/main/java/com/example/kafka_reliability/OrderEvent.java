package com.example.kafka_reliability;

public class OrderEvent {
    private String eventId;
    private String orderId;
    private String status;
    private boolean shouldFail;

    public OrderEvent() {
    }

    public OrderEvent(
            String eventId,
            String orderId,
            String status,
            boolean shouldFail) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.status = status;
        this.shouldFail = shouldFail;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isShouldFail() {
        return shouldFail;
    }

    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }
}
