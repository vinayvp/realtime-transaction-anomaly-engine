package com.anomaly.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("event_time")
    private Long eventTime;

    @JsonProperty("merchant_category")
    private String merchantCategory;

    @JsonProperty("location")
    private String location;

    public Transaction() {
    }

    public Transaction(String transactionId, String accountId, Double amount, Long eventTime, String merchantCategory, String location) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.eventTime = eventTime;
        this.merchantCategory = merchantCategory;
        this.location = location;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
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

    public String getMerchantCategory() {
        return merchantCategory;
    }

    public void setMerchantCategory(String merchantCategory) {
        this.merchantCategory = merchantCategory;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(accountId, that.accountId) &&
                Objects.equals(amount, that.amount) &&
                Objects.equals(eventTime, that.eventTime) &&
                Objects.equals(merchantCategory, that.merchantCategory) &&
                Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, accountId, amount, eventTime, merchantCategory, location);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", eventTime=" + eventTime +
                ", merchantCategory='" + merchantCategory + '\'' +
                ", location='" + location + '\'' +
                '}';
    }
}
