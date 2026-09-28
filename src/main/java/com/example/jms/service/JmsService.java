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
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.ArrayList;
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
        return testJndi(cfJndi, destJndi, null);
    }

    /**
     * Test lookup of ConnectionFactory, Destination, and optional TransactionManager in JNDI.
     */
    public Map<String, Object> testJndi(String cfJndi, String destJndi, String tmJndi) {
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
            boolean isXA = cfObj instanceof XAConnectionFactory;
            result.put("isXA", isXA);

            boolean xaAvailable = false;
            try {
                Object xaObj = ctx.lookup("XAConnectionFactory");
                xaAvailable = (xaObj instanceof XAConnectionFactory);
            } catch (Exception ignored) {}
            result.put("xaAvailable", xaAvailable);

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

            // 3. Check TransactionManager if provided
            if (tmJndi != null && !tmJndi.trim().isEmpty()) {
                Map<String, Object> tmResult = testTransactionManager(tmJndi);
                result.put("tmFound", tmResult.getOrDefault("tmFound", false));
                result.put("tmType", tmResult.getOrDefault("tmType", ""));
                result.put("tmStatus", tmResult.getOrDefault("tmStatus", ""));
                if (!Boolean.TRUE.equals(tmResult.get("success"))) {
                    result.put("tmError", tmResult.get("error"));
                }
            }

            result.put("success", true);
            StringBuilder msg = new StringBuilder("JNDI lookup succeeded for: " + cfJndi);
            if (destJndi != null && !destJndi.trim().isEmpty()) {
                msg.append(" and ").append(destJndi);
            }
            if (tmJndi != null && !tmJndi.trim().isEmpty()) {
                msg.append(" and TM: ").append(tmJndi);
            }
            result.put("message", msg.toString());
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
     * Test lookup of TransactionManager in JNDI (e.g., java:/TransactionManager).
     */
    public Map<String, Object> testTransactionManager(String tmJndi) {
        if (tmJndi == null || tmJndi.trim().isEmpty()) {
            tmJndi = "java:/TransactionManager";
        }
        tmJndi = tmJndi.trim();

        Map<String, Object> result = new HashMap<>();
        result.put("tmJndi", tmJndi);
        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
            Object tmObj = ctx.lookup(tmJndi);
            if (!(tmObj instanceof TransactionManager)) {
                result.put("success", false);
                result.put("error", "Found object at '" + tmJndi + "' but it is not a javax.transaction.TransactionManager (Type: " + (tmObj != null ? tmObj.getClass().getName() : "null") + ")");
                return result;
            }
            TransactionManager tm = (TransactionManager) tmObj;
            int status = tm.getStatus();
            result.put("success", true);
            result.put("tmFound", true);
            result.put("tmType", tmObj.getClass().getName());
            result.put("tmStatus", getStatusString(status));
            result.put("message", "TransactionManager located at '" + tmJndi + "' (" + tmObj.getClass().getName() + "), Status: " + getStatusString(status));
        } catch (NameNotFoundException e) {
            result.put("success", false);
            result.put("error", "JNDI NameNotFoundException for TransactionManager '" + tmJndi + "': " + e.getMessage() + ". Ensure JEUS Transaction Service is running.");
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", "Failed to lookup TransactionManager '" + tmJndi + "': " + e.getMessage());
        } finally {
            closeContext(ctx);
        }
        return result;
    }

    private static String getStatusString(int status) {
        switch (status) {
            case Status.STATUS_ACTIVE: return "STATUS_ACTIVE (0)";
            case Status.STATUS_MARKED_ROLLBACK: return "STATUS_MARKED_ROLLBACK (1)";
            case Status.STATUS_PREPARED: return "STATUS_PREPARED (2)";
            case Status.STATUS_COMMITTED: return "STATUS_COMMITTED (3)";
            case Status.STATUS_ROLLEDBACK: return "STATUS_ROLLEDBACK (4)";
            case Status.STATUS_UNKNOWN: return "STATUS_UNKNOWN (5)";
            case Status.STATUS_NO_TRANSACTION: return "STATUS_NO_TRANSACTION (6)";
            case Status.STATUS_PREPARING: return "STATUS_PREPARING (7)";
            case Status.STATUS_COMMITTING: return "STATUS_COMMITTING (8)";
            case Status.STATUS_ROLLING_BACK: return "STATUS_ROLLING_BACK (9)";
            default: return "STATUS_CODE_" + status;
        }
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

    /**
     * Send a batch of messages to a Queue within a Transaction (JTA Global or Local JMS Session).
     *
     * @param cfJndi ConnectionFactory JNDI name (e.g. "ConnectionFactory" or "XAConnectionFactory")
     * @param queueJndi Destination Queue JNDI name
     * @param text Base message text
     * @param priority JMS priority (0-9)
     * @param timeToLive Time to live in ms (0 = indefinite)
     * @param correlationId Optional correlation ID
     * @param properties Custom header properties
     * @param count Number of messages in batch (1-50)
     * @param txType "JTA" (via TransactionManager) or "LOCAL" (via session.commit)
     * @param simulateRollback If true, rollback the transaction; if false, commit
     * @param tmJndi JNDI name of TransactionManager (default "java:/TransactionManager")
     * @return Execution summary map
     */
    public Map<String, Object> sendQueueBatchTransactional(
            String cfJndi, String queueJndi, String text, int priority,
            long timeToLive, String correlationId, Map<String, String> properties,
            int count, String txType, boolean simulateRollback, String tmJndi) throws Exception {

        if (count < 1) count = 1;
        if (count > 50) count = 50; // Safety guard

        if (tmJndi == null || tmJndi.trim().isEmpty()) {
            tmJndi = "java:/TransactionManager";
        }
        tmJndi = tmJndi.trim();

        if (txType == null || txType.trim().isEmpty()) {
            txType = "JTA";
        }
        txType = txType.trim().toUpperCase();

        if ("LOCAL".equals(txType)) {
            return sendLocalTransactedBatch(cfJndi, queueJndi, text, priority, timeToLive, correlationId, properties, count, simulateRollback);
        } else {
            return sendJtaTransactedBatch(cfJndi, queueJndi, text, priority, timeToLive, correlationId, properties, count, simulateRollback, tmJndi);
        }
    }

    /**
     * JTA Distributed Transaction Batch Send using JEUS TransactionManager (java:/TransactionManager).
     */
    private Map<String, Object> sendJtaTransactedBatch(
            String cfJndi, String queueJndi, String text, int priority,
            long timeToLive, String correlationId, Map<String, String> properties,
            int count, boolean simulateRollback, String tmJndi) throws Exception {

        InitialContext ctx = null;
        TransactionManager tm = null;
        Connection conn = null;
        Session session = null;
        XAConnection xaConn = null;
        XASession xaSession = null;
        XAResource xaRes = null;
        MessageProducer producer = null;
        List<String> stagedMsgIds = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        String effectiveCfJndi = cfJndi;

        try {
            ctx = new InitialContext();

            // 1. Lookup TransactionManager from JNDI (e.g. java:/TransactionManager)
            Object tmObj = ctx.lookup(tmJndi);
            if (!(tmObj instanceof TransactionManager)) {
                throw new IllegalStateException("Found object at '" + tmJndi + "' but it is not a javax.transaction.TransactionManager (Type: " + (tmObj != null ? tmObj.getClass().getName() : "null") + ")");
            }
            tm = (TransactionManager) tmObj;

            // 2. Begin JTA Transaction
            tm.begin();
            Transaction tx = tm.getTransaction();

            // 3. Lookup Queue Destination
            Object destObj = ctx.lookup(queueJndi);
            if (destObj instanceof Topic) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is a Topic! Batch transactional send targets Queues.");
            }
            if (!(destObj instanceof Queue)) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is not a JMS Queue (Type: " + (destObj != null ? destObj.getClass().getName() : "null") + ")");
            }
            Queue queue = (Queue) destObj;

            // 4. Lookup ConnectionFactory & Enlist in Transaction
            // JTA Distributed Transaction strictly requires an XAConnectionFactory to coordinate 2PC with TransactionManager.
            Object cfObj = null;
            try {
                cfObj = ctx.lookup(cfJndi);
            } catch (Exception e) {
                LOGGER.log(Level.FINE, "Lookup for " + cfJndi + " failed, attempting fallback to XAConnectionFactory", e);
            }

            // If the looked-up object is NOT an XAConnectionFactory, auto-resolve "XAConnectionFactory" (standard in JEUS)
            if (!(cfObj instanceof XAConnectionFactory)) {
                try {
                    Object xaObj = ctx.lookup("XAConnectionFactory");
                    if (xaObj instanceof XAConnectionFactory) {
                        cfObj = xaObj;
                        effectiveCfJndi = "XAConnectionFactory";
                        LOGGER.info("Auto-resolved XAConnectionFactory from JNDI 'XAConnectionFactory' for JTA 2PC transaction.");
                    }
                } catch (Exception ignored) {}
            }

            // If still not an XAConnectionFactory, check if cfObj implements XAConnectionFactory under another common name
            if (!(cfObj instanceof XAConnectionFactory)) {
                String[] commonXaNames = {"jms/XAConnectionFactory", "java:comp/env/jms/XAConnectionFactory"};
                for (String name : commonXaNames) {
                    try {
                        Object xaCandidate = ctx.lookup(name);
                        if (xaCandidate instanceof XAConnectionFactory) {
                            cfObj = xaCandidate;
                            effectiveCfJndi = name;
                            break;
                        }
                    } catch (Exception ignored) {}
                }
            }

            if (!(cfObj instanceof XAConnectionFactory)) {
                throw new IllegalStateException("JTA transaction requires an XAConnectionFactory (e.g., 'XAConnectionFactory'). "
                        + "Object at '" + cfJndi + "' is " + (cfObj != null ? cfObj.getClass().getName() : "not found")
                        + ". Please configure or specify 'XAConnectionFactory' in JNDI settings.");
            }

            XAConnectionFactory xaCf = (XAConnectionFactory) cfObj;
            xaConn = xaCf.createXAConnection();
            xaSession = xaConn.createXASession();
            session = xaSession.getSession();
            xaRes = xaSession.getXAResource();

            if (tx != null && xaRes != null) {
                tx.enlistResource(xaRes);
            } else {
                throw new IllegalStateException("Unable to enlist XAResource into JTA Transaction (tx=" + tx + ", xaRes=" + xaRes + ")");
            }

            producer = session.createProducer(queue);
            if (priority >= 0 && priority <= 9) {
                producer.setPriority(priority);
            }
            if (timeToLive >= 0) {
                producer.setTimeToLive(timeToLive);
            }

            // 5. Send batch messages inside transaction boundary
            for (int i = 1; i <= count; i++) {
                String payload = (count == 1) ? text : text + " [JTA TX #" + i + "/" + count + "]";
                TextMessage message = session.createTextMessage(payload);

                if (correlationId != null && !correlationId.trim().isEmpty()) {
                    message.setJMSCorrelationID((count == 1) ? correlationId.trim() : correlationId.trim() + "-JTA" + i);
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
                message.setStringProperty("JMS_JEUS_TX_TYPE", "JTA");
                message.setStringProperty("JMS_JEUS_TM_JNDI", tmJndi);
                message.setStringProperty("JMS_JEUS_XA_CF_JNDI", effectiveCfJndi);

                producer.send(message);
                stagedMsgIds.add(message.getJMSMessageID());
            }

            // 6. Commit or Rollback according to simulateRollback flag
            if (simulateRollback) {
                // Disassociate XAResource with TMFAIL before rollback so JEUS marks the branch aborted
                if (xaRes != null && tx != null) {
                    try {
                        tx.delistResource(xaRes, XAResource.TMFAIL);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Warning during XAResource delist on rollback", e);
                    }
                }

                tm.rollback();

                String desc = "[JTA Rollback via " + tmJndi + "] Rolled back " + count + " staged message(s) via " + effectiveCfJndi + ". None persisted to queue.";
                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "TX_ROLLBACK", queueJndi, "TX-ABORTED", correlationId, priority, properties,
                        text + " (" + count + " rolled back)", "ROLLED_BACK", desc
                ));

                result.put("success", true);
                result.put("txType", "JTA");
                result.put("cfJndi", effectiveCfJndi);
                result.put("action", "ROLLBACK");
                result.put("committed", false);
                result.put("stagedCount", count);
                result.put("messageIds", stagedMsgIds);
                result.put("message", "Simulated rollback successful! tm.rollback() was invoked on " + tmJndi + " using " + effectiveCfJndi + ". " + count + " staged messages were discarded; 0 messages persisted to " + queueJndi + ".");
            } else {
                // Disassociate XAResource with TMSUCCESS before commit so JEUS marks the branch ready to commit
                if (xaRes != null && tx != null) {
                    try {
                        tx.delistResource(xaRes, XAResource.TMSUCCESS);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Warning during XAResource delist on commit", e);
                    }
                }

                tm.commit();

                String desc = "[JTA Commit via " + tmJndi + "] Committed " + count + " message(s) atomically to queue via " + effectiveCfJndi + ".";
                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "TX_COMMIT", queueJndi, stagedMsgIds.isEmpty() ? "TX-COMMITTED" : stagedMsgIds.get(0), correlationId, priority, properties,
                        text + " (" + count + " committed)", "COMMITTED", desc
                ));

                result.put("success", true);
                result.put("txType", "JTA");
                result.put("cfJndi", effectiveCfJndi);
                result.put("action", "COMMIT");
                result.put("committed", true);
                result.put("sentCount", count);
                result.put("messageIds", stagedMsgIds);
                result.put("message", "Transaction committed successfully! tm.commit() completed via " + tmJndi + " using " + effectiveCfJndi + ". " + count + " message(s) persisted atomically to " + queueJndi + ".");
            }

            return result;

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in JTA transactional batch send via " + tmJndi, e);
            try {
                if (xaRes != null && tm != null) {
                    try {
                        Transaction t = tm.getTransaction();
                        if (t != null) {
                            t.delistResource(xaRes, XAResource.TMFAIL);
                        }
                    } catch (Exception ignored) {}
                }
                if (tm != null && tm.getStatus() == Status.STATUS_ACTIVE) {
                    tm.rollback();
                }
            } catch (Exception rollbackEx) {
                LOGGER.log(Level.WARNING, "Failed to rollback JTA transaction", rollbackEx);
            }

            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "TX_ROLLBACK", queueJndi, "N/A", correlationId, priority, properties, text,
                    "ERROR", "Transaction failed and rolled back: " + e.getMessage()
            ));
            throw e;
        } finally {
            closeQuietly(producer);
            closeQuietly(session);
            if (xaSession != null) {
                try { xaSession.close(); } catch (Exception ignored) {}
            }
            if (xaConn != null) {
                try { xaConn.close(); } catch (Exception ignored) {}
            }
            closeQuietly(conn);
            closeContext(ctx);
        }
    }

    /**
     * Local JMS Session Transaction Batch Send (session.commit / session.rollback).
     */
    private Map<String, Object> sendLocalTransactedBatch(
            String cfJndi, String queueJndi, String text, int priority,
            long timeToLive, String correlationId, Map<String, String> properties,
            int count, boolean simulateRollback) throws Exception {

        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        MessageProducer producer = null;
        List<String> stagedMsgIds = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();

        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);
            Object destObj = ctx.lookup(queueJndi);
            if (!(destObj instanceof Queue)) {
                throw new IllegalArgumentException("Destination '" + queueJndi + "' is not a JMS Queue.");
            }
            Queue queue = (Queue) destObj;

            conn = cf.createConnection();
            // Create a transacted local session
            session = conn.createSession(true, Session.SESSION_TRANSACTED);
            producer = session.createProducer(queue);

            if (priority >= 0 && priority <= 9) producer.setPriority(priority);
            if (timeToLive >= 0) producer.setTimeToLive(timeToLive);

            for (int i = 1; i <= count; i++) {
                String payload = (count == 1) ? text : text + " [Local TX #" + i + "/" + count + "]";
                TextMessage message = session.createTextMessage(payload);

                if (correlationId != null && !correlationId.trim().isEmpty()) {
                    message.setJMSCorrelationID((count == 1) ? correlationId.trim() : correlationId.trim() + "-LTX" + i);
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
                message.setStringProperty("JMS_JEUS_TX_TYPE", "LOCAL_SESSION_TRANSACTED");

                producer.send(message);
                stagedMsgIds.add(message.getJMSMessageID());
            }

            if (simulateRollback) {
                session.rollback();
                String desc = "[Local JMS Rollback] Rolled back " + count + " message(s). None persisted to queue.";
                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "TX_ROLLBACK", queueJndi, "LOCAL-ROLLBACK", correlationId, priority, properties,
                        text + " (" + count + " rolled back)", "ROLLED_BACK", desc
                ));

                result.put("success", true);
                result.put("txType", "LOCAL");
                result.put("action", "ROLLBACK");
                result.put("committed", false);
                result.put("stagedCount", count);
                result.put("messageIds", stagedMsgIds);
                result.put("message", "Local JMS transaction rolled back via session.rollback(). " + count + " messages discarded; 0 persisted to queue.");
            } else {
                session.commit();
                String desc = "[Local JMS Commit] Committed " + count + " message(s) atomically to queue.";
                MessageActivityStore.getInstance().record(new ActivityRecord(
                        "TX_COMMIT", queueJndi, stagedMsgIds.isEmpty() ? "LOCAL-COMMIT" : stagedMsgIds.get(0), correlationId, priority, properties,
                        text + " (" + count + " committed)", "COMMITTED", desc
                ));

                result.put("success", true);
                result.put("txType", "LOCAL");
                result.put("action", "COMMIT");
                result.put("committed", true);
                result.put("sentCount", count);
                result.put("messageIds", stagedMsgIds);
                result.put("message", "Local JMS transaction committed via session.commit(). " + count + " message(s) persisted atomically to queue.");
            }

            return result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed in local transacted batch send: " + queueJndi, e);
            if (session != null) {
                try { session.rollback(); } catch (Exception ignored) {}
            }
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "TX_ROLLBACK", queueJndi, "N/A", correlationId, priority, properties, text,
                    "ERROR", "Local transaction error: " + e.getMessage()
            ));
            throw e;
        } finally {
            closeQuietly(producer);
            closeQuietly(session);
            closeQuietly(conn);
            closeContext(ctx);
        }
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
