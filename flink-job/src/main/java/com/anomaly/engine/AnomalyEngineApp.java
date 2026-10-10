package com.anomaly.engine;

import com.anomaly.engine.config.JobConfig;
import com.anomaly.engine.functions.DynamicFraudRuleEvaluator;
import com.anomaly.engine.model.Alert;
import com.anomaly.engine.model.Rule;
import com.anomaly.engine.model.Transaction;
import com.anomaly.engine.serialization.AlertSerializationSchema;
import com.anomaly.engine.serialization.JsonDeserializationSchema;
import com.anomaly.engine.serialization.TransactionSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

public class AnomalyEngineApp {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyEngineApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing Real-Time Transaction Anomaly & Dynamic Rule Engine...");

        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap.servers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", JobConfig.DEFAULT_BOOTSTRAP_SERVERS));

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 1. State Backend & Incremental Checkpoint Configuration
        LOG.info("Configuring EmbeddedRocksDBStateBackend with incremental checkpointing...");
        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDbBackend);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(params.get("checkpoint.dir", JobConfig.CHECKPOINT_DIR));
        checkpointConfig.setCheckpointInterval(JobConfig.CHECKPOINT_INTERVAL_MS);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(JobConfig.CHECKPOINT_TIMEOUT_MS);
        checkpointConfig.setMinPauseBetweenCheckpoints(JobConfig.CHECKPOINT_MIN_PAUSE_MS);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2. High-Throughput Transaction Source with Bounded-Out-Of-Orderness Watermarks
        LOG.info("Setting up Kafka transactions source on topic '{}'...", JobConfig.TOPIC_TRANSACTIONS);
        KafkaSource<Transaction> transactionSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(JobConfig.TOPIC_TRANSACTIONS)
                .setGroupId(JobConfig.CONSUMER_GROUP_TRANSACTIONS)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Transaction.class))
                .build();

        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(JobConfig.WATERMARK_OUT_OF_ORDERNESS_SEC))
                .withTimestampAssigner((tx, recordTimestamp) -> tx.getEventTime())
                .withIdleness(Duration.ofSeconds(1));

        DataStream<Transaction> transactionStream = env
                .fromSource(transactionSource, watermarkStrategy, "KafkaTransactionsSource")
                .name("KafkaTransactionsSource")
                .uid("kafka-transactions-source");

        // 3. Dynamic Rules Control Stream (Broadcast to all subtasks)
        LOG.info("Setting up Kafka fraud-rules control source on topic '{}'...", JobConfig.TOPIC_FRAUD_RULES);
        KafkaSource<Rule> ruleSource = KafkaSource.<Rule>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(JobConfig.TOPIC_FRAUD_RULES)
                .setGroupId(JobConfig.CONSUMER_GROUP_RULES)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Rule.class))
                .build();

        WatermarkStrategy<Rule> ruleWatermarkStrategy = WatermarkStrategy
                .<Rule>noWatermarks()
                .withIdleness(Duration.ofSeconds(1));

        DataStream<Rule> ruleStream = env
                .fromSource(ruleSource, ruleWatermarkStrategy, "KafkaFraudRulesSource")
                .name("KafkaFraudRulesSource")
                .uid("kafka-fraud-rules-source");

        BroadcastStream<Rule> broadcastRulesStream = ruleStream.broadcast(DynamicFraudRuleEvaluator.RULES_DESCRIPTOR);

        // 4. Connect streams and apply KeyedBroadcastProcessFunction
        SingleOutputStreamOperator<Alert> alertsStream = transactionStream
                .keyBy(Transaction::getAccountId)
                .connect(broadcastRulesStream)
                .process(new DynamicFraudRuleEvaluator())
                .name("DynamicFraudRuleEvaluator")
                .uid("dynamic-fraud-rule-evaluator");

        // 5. Late Data Side-Output Sink -> transactions-dlq
        DataStream<Transaction> lateTransactionStream = alertsStream.getSideOutput(
                DynamicFraudRuleEvaluator.LATE_TRANSACTION_OUTPUT_TAG);

        KafkaSink<Transaction> dlqSink = KafkaSink.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new TransactionSerializationSchema(JobConfig.TOPIC_TRANSACTIONS_DLQ))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(JobConfig.DLQ_TRANSACTIONAL_ID_PREFIX)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, JobConfig.KAFKA_TRANSACTION_TIMEOUT_MS)
                .build();

        lateTransactionStream
                .sinkTo(dlqSink)
                .name("TransactionsDLQSink")
                .uid("transactions-dlq-sink");

        // 6. Alert Sink -> fraud-alerts with Exactly-Once Semantics (2PC)
        KafkaSink<Alert> alertSink = KafkaSink.<Alert>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new AlertSerializationSchema(JobConfig.TOPIC_FRAUD_ALERTS))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(JobConfig.ALERT_TRANSACTIONAL_ID_PREFIX)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, JobConfig.KAFKA_TRANSACTION_TIMEOUT_MS)
                .build();

        alertsStream
                .sinkTo(alertSink)
                .name("FraudAlertsSink")
                .uid("fraud-alerts-sink");

        LOG.info("Execution topology wired successfully. Submitting Flink Job...");
        env.execute("Real-Time Transaction Anomaly & Dynamic Rule Engine");
    }
}
