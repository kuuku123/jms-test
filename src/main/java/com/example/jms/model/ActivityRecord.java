package com.example.jms.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a single JMS event or message activity record in the console.
 */
public class ActivityRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String timestamp;
    private final String action;       // SEND, RECEIVE, BROWSE, PUBLISH, ASYNC_RECV, ERROR
    private final String destination;  // JNDI Destination Name
    private final String messageId;
    private final String correlationId;
    private final int priority;
    private final Map<String, String> properties;
    private final String payload;
    private final String status;      // SUCCESS, EMPTY, ERROR
    private final String details;

    public ActivityRecord(String action, String destination, String messageId, String correlationId,
                          int priority, Map<String, String> properties, String payload,
                          String status, String details) {
        this.id = UUID.randomUUID().toString().substring(0, 8);
        this.timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
        this.action = action;
        this.destination = destination != null ? destination : "N/A";
        this.messageId = messageId != null ? messageId : "N/A";
        this.correlationId = correlationId != null ? correlationId : "";
        this.priority = priority;
        this.properties = properties != null ? new HashMap<>(properties) : Collections.emptyMap();
        this.payload = payload != null ? payload : "";
        this.status = status != null ? status : "SUCCESS";
        this.details = details != null ? details : "";
    }

    public String getId() {
        return id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getAction() {
        return action;
    }

    public String getDestination() {
        return destination;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public int getPriority() {
        return priority;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public String getPayload() {
        return payload;
    }

    public String getStatus() {
        return status;
    }

    public String getDetails() {
        return details;
    }
}
