package com.anomaly.engine.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonDeserializationSchema.class);

    private final Class<T> targetClass;
    private transient ObjectMapper objectMapper;

    public JsonDeserializationSchema(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            return getObjectMapper().readValue(message, targetClass);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize JSON message into target class {}: {}", targetClass.getSimpleName(), e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetClass);
    }
}
