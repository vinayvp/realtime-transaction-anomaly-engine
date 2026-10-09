package com.anomaly.engine.serialization;

import com.anomaly.engine.model.Alert;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class AlertSerializationSchema implements KafkaRecordSerializationSchema<Alert> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertSerializationSchema.class);

    private final String topic;
    private transient ObjectMapper objectMapper;

    public AlertSerializationSchema(String topic) {
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
    public ProducerRecord<byte[], byte[]> serialize(Alert alert, KafkaSinkContext context, Long timestamp) {
        if (alert == null) {
            return null;
        }
        try {
            byte[] value = getObjectMapper().writeValueAsBytes(alert);
            byte[] key = alert.getAccountId() != null ? alert.getAccountId().getBytes(StandardCharsets.UTF_8) : null;
            return new ProducerRecord<>(topic, null, alert.getEventTime(), key, value);
        } catch (Exception e) {
            LOG.error("Failed to serialize Alert for topic {}: {}", topic, alert, e);
            return null;
        }
    }
}
