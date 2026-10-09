package com.anomaly.engine.serialization;

import com.anomaly.engine.model.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class TransactionSerializationSchema implements KafkaRecordSerializationSchema<Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionSerializationSchema.class);

    private final String topic;
    private transient ObjectMapper objectMapper;

    public TransactionSerializationSchema(String topic) {
        this.topic = topic;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Transaction tx, KafkaSinkContext context, Long timestamp) {
        if (tx == null) {
            return null;
        }
        try {
            byte[] value = getObjectMapper().writeValueAsBytes(tx);
            byte[] key = tx.getAccountId() != null ? tx.getAccountId().getBytes(StandardCharsets.UTF_8) : null;
            return new ProducerRecord<>(topic, null, tx.getEventTime(), key, value);
        } catch (Exception e) {
            LOG.error("Failed to serialize Transaction for DLQ topic {}: {}", topic, tx, e);
            return null;
        }
    }
}
