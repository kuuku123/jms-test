package com.example.jms.service;

import com.example.jms.model.ActivityRecord;
import com.example.jms.model.BrowsedMessage;
import com.example.jms.model.MessageActivityStore;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Core JMS service handling JNDI lookups, queue operations (send, browse, receive),
 * and topic operations (publish) on JEUS 8.5.
 */
public class JmsService {

    private static final Logger LOGGER = Logger.getLogger(JmsService.class.getName());
    private static final JmsService INSTANCE = new JmsService();

    private JmsService() {}

    public static JmsService getInstance() {
        return INSTANCE;
    }

    /**
     * Test lookup of ConnectionFactory and optional Destination in JNDI.
     */
    public Map<String, Object> testJndi(String cfJndi, String destJndi) {
        Map<String, Object> result = new HashMap<>();
        InitialContext ctx = null;
        try {
            ctx = new InitialContext();

            // 1. Check ConnectionFactory
            Object cfObj = ctx.lookup(cfJndi);
            if (!(cfObj instanceof ConnectionFactory)) {
                result.put("success", false);
                result.put("error", "Found object at '" + cfJndi + "' but it is not a JMS ConnectionFactory (Type: " + cfObj.getClass().getName() + ")");
                return result;
            }
            result.put("cfFound", true);
            result.put("cfType", cfObj.getClass().getName());

            // 2. Check Destination if provided
            if (destJndi != null && !destJndi.trim().isEmpty()) {
                Object destObj = ctx.lookup(destJndi.trim());
                if (destObj instanceof Queue) {
                    result.put("destFound", true);
                    result.put("destType", "Queue (" + destObj.getClass().getName() + ")");
                    result.put("destCategory", "QUEUE");
                } else if (destObj instanceof Topic) {
                    result.put("destFound", true);
                    result.put("destType", "Topic (" + destObj.getClass().getName() + ")");
                    result.put("destCategory", "TOPIC");
                } else {
                    result.put("destFound", true);
                    result.put("destType", "Destination (" + destObj.getClass().getName() + ")");
                    result.put("destCategory", "UNKNOWN");
                }
            }

            result.put("success", true);
            result.put("message", "JNDI lookup succeeded for: " + cfJndi + (destJndi != null && !destJndi.trim().isEmpty() ? " and " + destJndi : ""));
        } catch (NameNotFoundException e) {
            result.put("success", false);
            result.put("error", "JNDI NameNotFoundException: " + e.getMessage() + ". Please check JEUS WebAdmin > JMS > Resources to verify registered names.");
        } catch (NamingException e) {
            result.put("success", false);
            result.put("error", "NamingException: " + e.getMessage());
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", "Lookup error: " + e.getMessage());
        } finally {
            closeContext(ctx);
        }
        return result;
    }

    /**
     * Send one or more messages to a Queue.
     */
    public List<String> sendQueueMessage(String cfJndi, String queueJndi, String text, int priority,
                                         long timeToLive, String correlationId, Map<String, String> properties,
                                         int count) throws Exception {
        if (count < 1) count = 1;
        if (count > 50) count = 50; // Safety guard

        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        MessageProducer producer = null;
        List<String> sentMessageIds = new ArrayList<>();

        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);

            Object destObj = ctx.lookup(queueJndi);
            if (destObj instanceof Topic) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is a Topic! Use Topic Studio to broadcast to topics.");
            }
            if (!(destObj instanceof Queue)) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is not a JMS Queue (Type: " + (destObj != null ? destObj.getClass().getName() : "null") + ")");
            }
            Queue queue = (Queue) destObj;

            conn = cf.createConnection();
            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(queue);

            if (priority >= 0 && priority <= 9) {
                producer.setPriority(priority);
            }
            if (timeToLive >= 0) {
                producer.setTimeToLive(timeToLive);
            }

            for (int i = 1; i <= count; i++) {
                String payload = (count == 1) ? text : text + " [#" + i + "]";
                TextMessage message = session.createTextMessage(payload);

                if (correlationId != null && !correlationId.trim().isEmpty()) {
                    message.setJMSCorrelationID((count == 1) ? correlationId.trim() : correlationId.trim() + "-" + i);
                }

                if (properties != null) {
                    for (Map.Entry<String, String> entry : properties.entrySet()) {
                        String key = entry.getKey().trim();
                        String val = entry.getValue() != null ? entry.getValue().trim() : "";
                        if (!key.isEmpty()) {
                            message.setStringProperty(key, val);
                        }
                    }
                }

                producer.send(message);
                String msgId = message.getJMSMessageID();
                sentMessageIds.add(msgId);

                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "SEND", queueJndi, msgId, message.getJMSCorrelationID(),
                        message.getJMSPriority(), properties, payload, "SUCCESS",
                        "Sent message to queue (TTL: " + timeToLive + "ms)"
                ));
            }

            return sentMessageIds;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to send message to queue: " + queueJndi, e);
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "SEND", queueJndi, "N/A", correlationId, priority, properties, text,
                    "ERROR", e.getMessage()
            ));
            throw e;
        } finally {
            closeQuietly(producer);
            closeQuietly(session);
            closeQuietly(conn);
            closeContext(ctx);
        }
    }

    /**
     * Browse pending messages in a queue without removing them.
     */
    public List<BrowsedMessage> browseQueue(String cfJndi, String queueJndi, String selector) throws Exception {
        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        QueueBrowser browser = null;
        List<BrowsedMessage> result = new ArrayList<>();

        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);

            Object destObj = ctx.lookup(queueJndi);
            if (destObj instanceof Topic) {
                throw new IllegalArgumentException("Cannot browse a Topic (" + queueJndi + ")! JMS QueueBrowser only supports Point-to-Point Queues. To monitor topic messages in real-time, use the Dynamic Topic Subscriber in Topic Studio.");
            }
            if (!(destObj instanceof Queue)) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is not a JMS Queue (Type: " + (destObj != null ? destObj.getClass().getName() : "null") + ")");
            }
            Queue queue = (Queue) destObj;

            conn = cf.createConnection();
            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            if (selector != null && !selector.trim().isEmpty()) {
                browser = session.createBrowser(queue, selector.trim());
            } else {
                browser = session.createBrowser(queue);
            }

            Enumeration<?> messages = browser.getEnumeration();
            int count = 0;
            while (messages.hasMoreElements() && count < 100) { // Cap at 100
                Object item = messages.nextElement();
                if (item instanceof Message) {
                    Message msg = (Message) item;
                    Map<String, String> props = extractProperties(msg);
                    String payload = extractPayload(msg);

                    result.add(new BrowsedMessage(
                            msg.getJMSMessageID(),
                            msg.getJMSCorrelationID(),
                            msg.getJMSTimestamp(),
                            msg.getJMSPriority(),
                            msg.getJMSDeliveryMode(),
                            msg.getJMSExpiration(),
                            msg.getJMSRedelivered(),
                            props,
                            payload
                    ));
                    count++;
                }
            }

            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "BROWSE", queueJndi, "N/A", "", 0, null, "", "SUCCESS",
                    "Browsed " + result.size() + " message(s)" + (selector != null && !selector.trim().isEmpty() ? " [Selector: " + selector + "]" : "")
            ));

            return result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to browse queue: " + queueJndi, e);
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "BROWSE", queueJndi, "N/A", "", 0, null, "", "ERROR", e.getMessage()
            ));
            throw e;
        } finally {
            closeQuietly(browser);
            closeQuietly(session);
            closeQuietly(conn);
            closeContext(ctx);
        }
    }

    /**
     * Synchronously receive a message from a queue (with timeout or noWait).
     */
    public BrowsedMessage receiveQueueMessage(String cfJndi, String queueJndi, long timeoutMillis, String selector) throws Exception {
        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);

            Object destObj = ctx.lookup(queueJndi);
            if (destObj instanceof Topic) {
                throw new IllegalArgumentException("Cannot synchronously receive from a Topic (" + queueJndi + ") using Queue Consumer. In JMS Pub/Sub, start the Dynamic Topic Subscriber in Topic Studio before publishing messages.");
            }
            if (!(destObj instanceof Queue)) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is not a JMS Queue (Type: " + (destObj != null ? destObj.getClass().getName() : "null") + ")");
            }
            Queue queue = (Queue) destObj;

            conn = cf.createConnection();
            conn.start(); // Required to start delivery of messages!

            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            if (selector != null && !selector.trim().isEmpty()) {
                consumer = session.createConsumer(queue, selector.trim());
            } else {
                consumer = session.createConsumer(queue);
            }

            Message msg;
            if (timeoutMillis > 0) {
                msg = consumer.receive(timeoutMillis);
            } else {
                msg = consumer.receiveNoWait();
            }

            if (msg != null) {
                Map<String, String> props = extractProperties(msg);
                String payload = extractPayload(msg);

                BrowsedMessage received = new BrowsedMessage(
                        msg.getJMSMessageID(),
                        msg.getJMSCorrelationID(),
                        msg.getJMSTimestamp(),
                        msg.getJMSPriority(),
                        msg.getJMSDeliveryMode(),
                        msg.getJMSExpiration(),
                        msg.getJMSRedelivered(),
                        props,
                        payload
                );

                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "RECEIVE", queueJndi, received.getMessageId(), received.getCorrelationId(),
                        received.getPriority(), props, payload, "SUCCESS",
                        "Consumed message from queue"
                ));

                return received;
            } else {
                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "RECEIVE", queueJndi, "N/A", "", 0, null, "", "EMPTY",
                        "Queue empty or receive timeout (" + timeoutMillis + "ms)"
                ));
                return null;
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to receive from queue: " + queueJndi, e);
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "RECEIVE", queueJndi, "N/A", "", 0, null, "", "ERROR", e.getMessage()
            ));
            throw e;
        } finally {
            closeQuietly(consumer);
            closeQuietly(session);
            closeQuietly(conn);
            closeContext(ctx);
        }
    }

    /**
     * Publish one or more messages to a Topic.
     */
    public List<String> publishTopicMessage(String cfJndi, String topicJndi, String text, int priority,
                                            long timeToLive, String correlationId, Map<String, String> properties,
                                            int count) throws Exception {
        if (count < 1) count = 1;
        if (count > 50) count = 50;

        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        MessageProducer producer = null;
        List<String> sentMessageIds = new ArrayList<>();

        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);

            Object destObj = ctx.lookup(topicJndi);
            if (!(destObj instanceof Topic)) {
                if (destObj instanceof Queue) {
                    throw new IllegalArgumentException("Destination '" + topicJndi + "' is a Queue, not a Topic! Use Queue Studio to send to queues.");
                }
                throw new IllegalArgumentException("Destination '" + topicJndi + "' is not a JMS Topic (Type: " + (destObj != null ? destObj.getClass().getName() : "null") + ")");
            }
            Topic topic = (Topic) destObj;

            conn = cf.createConnection();
            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(topic);

            if (priority >= 0 && priority <= 9) {
                producer.setPriority(priority);
            }
            if (timeToLive >= 0) {
                producer.setTimeToLive(timeToLive);
            }

            for (int i = 1; i <= count; i++) {
                String payload = (count == 1) ? text : text + " [#" + i + "]";
                TextMessage message = session.createTextMessage(payload);

                if (correlationId != null && !correlationId.trim().isEmpty()) {
                    message.setJMSCorrelationID((count == 1) ? correlationId.trim() : correlationId.trim() + "-" + i);
                }

                if (properties != null) {
                    for (Map.Entry<String, String> entry : properties.entrySet()) {
                        String key = entry.getKey().trim();
                        String val = entry.getValue() != null ? entry.getValue().trim() : "";
                        if (!key.isEmpty()) {
                            message.setStringProperty(key, val);
                        }
                    }
                }

                producer.send(message);
                String msgId = message.getJMSMessageID();
                sentMessageIds.add(msgId);

                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "PUBLISH", topicJndi, msgId, message.getJMSCorrelationID(),
                        message.getJMSPriority(), properties, payload, "SUCCESS",
                        "Broadcast to topic (TTL: " + timeToLive + "ms)"
                ));
            }

            return sentMessageIds;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to publish to topic: " + topicJndi, e);
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "PUBLISH", topicJndi, "N/A", correlationId, priority, properties, text,
                    "ERROR", e.getMessage()
            ));
            throw e;
        } finally {
            closeQuietly(producer);
            closeQuietly(session);
            closeQuietly(conn);
            closeContext(ctx);
        }
    }

    /**
     * Convenience method to publish a single message to a Topic.
     */
    public String publishTopicMessage(String cfJndi, String topicJndi, String text, int priority,
                                      long timeToLive, String correlationId, Map<String, String> properties) throws Exception {
        List<String> ids = publishTopicMessage(cfJndi, topicJndi, text, priority, timeToLive, correlationId, properties, 1);
        return ids.isEmpty() ? null : ids.get(0);
    }

    public static Map<String, String> extractProperties(Message message) {
        Map<String, String> props = new HashMap<>();
        try {
            Enumeration<?> names = message.getPropertyNames();
            while (names != null && names.hasMoreElements()) {
                String name = String.valueOf(names.nextElement());
                Object val = message.getObjectProperty(name);
                props.put(name, val != null ? String.valueOf(val) : "");
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Could not extract properties from message", e);
        }
        return props;
    }

    public static String extractPayload(Message message) {
        try {
            if (message instanceof TextMessage) {
                return ((TextMessage) message).getText();
            }
            return "[" + message.getClass().getSimpleName() + " payload]";
        } catch (Exception e) {
            return "[Error reading payload: " + e.getMessage() + "]";
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {}
        }
    }

    private static void closeContext(InitialContext ctx) {
        if (ctx != null) {
            try {
                ctx.close();
            } catch (Exception ignored) {}
        }
    }
}
