package com.anomaly.engine.model;

import java.io.Serializable;

public enum RuleType implements Serializable {
    SINGLE_TRANSACTION_THRESHOLD,
    SLIDING_WINDOW_ACCUMULATION,
    HIGH_FREQUENCY_BURST
}
