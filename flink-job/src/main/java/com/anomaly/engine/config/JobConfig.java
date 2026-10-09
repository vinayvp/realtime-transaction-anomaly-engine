package com.anomaly.engine.config;

import java.io.Serializable;

public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";
    public static final String TOPIC_TRANSACTIONS = "transactions";
    public static final String TOPIC_FRAUD_RULES = "fraud-rules";
    public static final String TOPIC_FRAUD_ALERTS = "fraud-alerts";
    public static final String TOPIC_TRANSACTIONS_DLQ = "transactions-dlq";

    public static final String CONSUMER_GROUP_TRANSACTIONS = "flink-transaction-consumers";
    public static final String CONSUMER_GROUP_RULES = "flink-rule-broadcast-consumers";

    public static final String CHECKPOINT_DIR = "file:///opt/flink/checkpoints";
    public static final long CHECKPOINT_INTERVAL_MS = 10000L;
    public static final long CHECKPOINT_TIMEOUT_MS = 60000L;
    public static final long CHECKPOINT_MIN_PAUSE_MS = 5000L;

    public static final long WATERMARK_OUT_OF_ORDERNESS_SEC = 10L;
    public static final long WATERMARK_IDLENESS_SEC = 5L;

    public static final String ALERT_TRANSACTIONAL_ID_PREFIX = "fraud-alert-sink-";
    public static final String DLQ_TRANSACTIONAL_ID_PREFIX = "fraud-dlq-sink-";
    public static final String KAFKA_TRANSACTION_TIMEOUT_MS = "900000"; // 15 mins
}
