package com.anomaly.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Rule implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("rule_id")
    private String ruleId;

    @JsonProperty("rule_type")
    private RuleType ruleType;

    @JsonProperty("threshold")
    private Double threshold;

    @JsonProperty("window_minutes")
    private Integer windowMinutes;

    @JsonProperty("is_active")
    private Boolean isActive;

    public Rule() {
    }

    public Rule(String ruleId, RuleType ruleType, Double threshold, Integer windowMinutes, Boolean isActive) {
        this.ruleId = ruleId;
        this.ruleType = ruleType;
        this.threshold = threshold;
        this.windowMinutes = windowMinutes;
        this.isActive = isActive;
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

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }

    public Integer getWindowMinutes() {
        return windowMinutes;
    }

    public void setWindowMinutes(Integer windowMinutes) {
        this.windowMinutes = windowMinutes;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public boolean isActive() {
        return isActive != null && isActive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rule rule = (Rule) o;
        return Objects.equals(ruleId, rule.ruleId) &&
                ruleType == rule.ruleType &&
                Objects.equals(threshold, rule.threshold) &&
                Objects.equals(windowMinutes, rule.windowMinutes) &&
                Objects.equals(isActive, rule.isActive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, ruleType, threshold, windowMinutes, isActive);
    }

    @Override
    public String toString() {
        return "Rule{" +
                "ruleId='" + ruleId + '\'' +
                ", ruleType=" + ruleType +
                ", threshold=" + threshold +
                ", windowMinutes=" + windowMinutes +
                ", isActive=" + isActive +
                '}';
    }
}
