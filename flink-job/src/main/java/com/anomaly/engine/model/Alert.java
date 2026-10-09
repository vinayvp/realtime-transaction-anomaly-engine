package com.anomaly.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("alert_id")
    private String alertId;

    @JsonProperty("rule_id")
    private String ruleId;

    @JsonProperty("rule_type")
    private RuleType ruleType;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("trigger_value")
    private Double triggerValue;

    @JsonProperty("event_time")
    private Long eventTime;

    @JsonProperty("detection_time")
    private Long detectionTime;

    @JsonProperty("details")
    private String details;

    public Alert() {
    }

    public Alert(String alertId, String ruleId, RuleType ruleType, String transactionId,
                 String accountId, Double amount, Double triggerValue, Long eventTime,
                 Long detectionTime, String details) {
        this.alertId = alertId;
        this.ruleId = ruleId;
        this.ruleType = ruleType;
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.triggerValue = triggerValue;
        this.eventTime = eventTime;
        this.detectionTime = detectionTime;
        this.details = details;
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public RuleType getRuleType() {
        return ruleType;
    }

    public void setRuleType(RuleType ruleType) {
        this.ruleType = ruleType;
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

    public Double getTriggerValue() {
        return triggerValue;
    }

    public void setTriggerValue(Double triggerValue) {
        this.triggerValue = triggerValue;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getDetectionTime() {
        return detectionTime;
    }

    public void setDetectionTime(Long detectionTime) {
        this.detectionTime = detectionTime;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return Objects.equals(alertId, alert.alertId) &&
                Objects.equals(ruleId, alert.ruleId) &&
                ruleType == alert.ruleType &&
                Objects.equals(transactionId, alert.transactionId) &&
                Objects.equals(accountId, alert.accountId) &&
                Objects.equals(amount, alert.amount) &&
                Objects.equals(triggerValue, alert.triggerValue) &&
                Objects.equals(eventTime, alert.eventTime) &&
                Objects.equals(detectionTime, alert.detectionTime) &&
                Objects.equals(details, alert.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId, ruleId, ruleType, transactionId, accountId, amount, triggerValue, eventTime, detectionTime, details);
    }

    @Override
    public String toString() {
        return "Alert{" +
                "alertId='" + alertId + '\'' +
                ", ruleId='" + ruleId + '\'' +
                ", ruleType=" + ruleType +
                ", transactionId='" + transactionId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", triggerValue=" + triggerValue +
                ", eventTime=" + eventTime +
                ", detectionTime=" + detectionTime +
                ", details='" + details + '\'' +
                '}';
    }
}
