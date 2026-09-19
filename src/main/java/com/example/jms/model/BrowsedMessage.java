package com.example.jms.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the metadata and payload of a message inspected via QueueBrowser.
 */
public class BrowsedMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String messageId;
    private final String correlationId;
    private final String timestamp;
    private final int priority;
    private final String deliveryMode;
    private final long expiration;
    private final boolean redelivered;
    private final Map<String, String> properties;
    private final String payload;

    public BrowsedMessage(String messageId, String correlationId, long timestampMillis,
                          int priority, int deliveryMode, long expiration, boolean redelivered,
                          Map<String, String> properties, String payload) {
        this.messageId = messageId != null ? messageId : "";
        this.correlationId = correlationId != null ? correlationId : "";
        this.timestamp = timestampMillis > 0 ?
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(timestampMillis)) : "N/A";
        this.priority = priority;
        this.deliveryMode = (deliveryMode == 1) ? "NON_PERSISTENT" : "PERSISTENT";
        this.expiration = expiration;
        this.redelivered = redelivered;
        this.properties = properties != null ? new HashMap<>(properties) : Collections.emptyMap();
        this.payload = payload != null ? payload : "";
    }

    public String getMessageId() {
        return messageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public int getPriority() {
        return priority;
    }

    public String getDeliveryMode() {
        return deliveryMode;
    }

    public long getExpiration() {
        return expiration;
    }

    public boolean isRedelivered() {
        return redelivered;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public String getPayload() {
        return payload;
    }
}
