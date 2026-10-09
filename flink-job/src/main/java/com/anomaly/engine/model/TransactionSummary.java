package com.anomaly.engine.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class TransactionSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("event_time")
    private Long eventTime;

    public TransactionSummary() {
    }

    public TransactionSummary(Double amount, Long eventTime) {
        this.amount = amount;
        this.eventTime = eventTime;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionSummary that = (TransactionSummary) o;
        return Objects.equals(amount, that.amount) &&
                Objects.equals(eventTime, that.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(amount, eventTime);
    }

    @Override
    public String toString() {
        return "TransactionSummary{" +
                "amount=" + amount +
                ", eventTime=" + eventTime +
                '}';
    }
}
