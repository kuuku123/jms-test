package com.example.jms.service;

import com.example.jms.model.ActivityRecord;
import com.example.jms.model.MessageActivityStore;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages on-demand, web-controlled asynchronous JMS MessageListeners for both Queues and Topics.
 * Supports concurrent listeners, non-durable and durable topic subscriptions, SQL-92 message selectors,
 * and graceful resource cleanup.
 */
public class DynamicAsyncListenerService {

    private static final Logger LOGGER = Logger.getLogger(DynamicAsyncListenerService.class.getName());
    private static final DynamicAsyncListenerService INSTANCE = new DynamicAsyncListenerService();

    public static class ActiveListenerContext {
        private final String cfJndi;
        private final String destJndi;
        private final String destType; // "QUEUE" or "TOPIC"
        private final String selector;
        private final boolean durable;
        private final String clientId;
        private final String subscriptionName;
        private final long startedAt;
        private final Connection connection;
        private final Session session;
        private final MessageConsumer consumer;

        public ActiveListenerContext(String cfJndi, String destJndi, String destType, String selector,
                                     boolean durable, String clientId, String subscriptionName,
                                     long startedAt, Connection connection, Session session, MessageConsumer consumer) {
            this.cfJndi = cfJndi;
            this.destJndi = destJndi;
            this.destType = destType;
            this.selector = selector;
            this.durable = durable;
            this.clientId = clientId;
            this.subscriptionName = subscriptionName;
            this.startedAt = startedAt;
            this.connection = connection;
            this.session = session;
            this.consumer = consumer;
        }

        public String getCfJndi() { return cfJndi; }
        public String getDestJndi() { return destJndi; }
        public String getDestType() { return destType; }
        public String getSelector() { return selector; }
        public boolean isDurable() { return durable; }
        public String getClientId() { return clientId; }
        public String getSubscriptionName() { return subscriptionName; }
        public long getStartedAt() { return startedAt; }

        public void close() {
            if (consumer != null) {
                try { consumer.close(); } catch (Exception ignored) {}
            }
            if (session != null) {
                try { session.close(); } catch (Exception ignored) {}
            }
            if (connection != null) {
                markConnectionDirtyAndClose(connection);
            }
        }
    }

    private final Map<String, ActiveListenerContext> activeListeners = new ConcurrentHashMap<>();

    private DynamicAsyncListenerService() {}

    public static DynamicAsyncListenerService getInstance() {
        return INSTANCE;
    }

    public boolean isRunning() {
        return !activeListeners.isEmpty();
    }

    public boolean isRunning(String destJndi) {
        if (destJndi == null) return false;
        return activeListeners.containsKey(destJndi.trim());
    }

    public String getCurrentDestination() {
        if (activeListeners.isEmpty()) return null;
        return String.join(", ", activeListeners.keySet());
    }

    public Set<String> getActiveDestinations() {
        return Collections.unmodifiableSet(new HashSet<>(activeListeners.keySet()));
    }

    public Map<String, ActiveListenerContext> getActiveListeners() {
        return Collections.unmodifiableMap(activeListeners);
    }

    /**
     * Start listening asynchronously on the given Destination (Queue or Topic).
     */
    public synchronized void start(String cfJndi, String destJndi) throws Exception {
        start(cfJndi, destJndi, null, false, null, null);
    }

    /**
     * Start listening asynchronously on the given Destination with optional selector.
     */
    public synchronized void start(String cfJndi, String destJndi, String selector) throws Exception {
        start(cfJndi, destJndi, selector, false, null, null);
    }

    /**
     * Start listening asynchronously on the given Destination (Queue or Topic) with full durable subscription options.
     */
    public synchronized void start(String cfJndi, String destJndi, String selector,
                                  boolean durable, String clientId, String subscriptionName) throws Exception {
        if (destJndi == null || destJndi.trim().isEmpty()) {
            throw new IllegalArgumentException("Destination JNDI name cannot be empty.");
        }
        destJndi = destJndi.trim();

        // If already listening on this destination, stop previous listener
        if (activeListeners.containsKey(destJndi)) {
            stop(destJndi);
        }

        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            ctx = new InitialContext();
            Object cfObj = ctx.lookup(cfJndi);
            if (!(cfObj instanceof ConnectionFactory)) {
                throw new IllegalArgumentException("Object at '" + cfJndi + "' is not a JMS ConnectionFactory.");
            }
            ConnectionFactory cf = (ConnectionFactory) cfObj;

            Object destObj = ctx.lookup(destJndi);
            if (!(destObj instanceof Destination)) {
                throw new IllegalArgumentException("Object at '" + destJndi + "' is not a JMS Destination (Type: " + destObj.getClass().getName() + ").");
            }
            Destination destination = (Destination) destObj;
            final boolean isTopic = (destination instanceof Topic);
            final String destType = isTopic ? "TOPIC" : "QUEUE";
            final String finalDestJndi = destJndi;

            final String effectiveClientId = (clientId != null && !clientId.trim().isEmpty())
                    ? clientId.trim() : "JmsStudioClient-01";
            final String effectiveSubName = (subscriptionName != null && !subscriptionName.trim().isEmpty())
                    ? subscriptionName.trim() : "TopicSub-01";

            // ClientID MUST be set before starting connection or creating sessions if using durable subscription.
            // JEUS pooled connections may have registerable=false if previously used to publish;
            // createConnectionWithClientId discards stale pooled connections and acquires a fresh one.
            if (durable && isTopic) {
                conn = createConnectionWithClientId(cf, effectiveClientId);
            } else {
                conn = cf.createConnection();
            }

            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            if (durable && isTopic) {
                Topic topic = (Topic) destination;
                if (selector != null && !selector.trim().isEmpty()) {
                    consumer = session.createDurableSubscriber(topic, effectiveSubName, selector.trim(), false);
                } else {
                    consumer = session.createDurableSubscriber(topic, effectiveSubName);
                }
            } else {
                if (selector != null && !selector.trim().isEmpty()) {
                    consumer = session.createConsumer(destination, selector.trim());
                } else {
                    consumer = session.createConsumer(destination);
                }
            }

            final boolean isDurableSub = (durable && isTopic);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        String msgId = message.getJMSMessageID();
                        String corrId = message.getJMSCorrelationID();
                        int priority = message.getJMSPriority();
                        Map<String, String> props = JmsService.extractProperties(message);
                        String payload = JmsService.extractPayload(message);

                        String action = isTopic ? "TOPIC_RECV" : "ASYNC_RECV";
                        String details = isTopic
                                ? ("Received by " + (isDurableSub ? "Durable " : "") + "Topic Subscriber on " + finalDestJndi + (isDurableSub ? " [" + effectiveSubName + "]" : ""))
                                : ("Received by Dynamic Background Consumer on " + finalDestJndi);

                        MessageActivityStore.getInstance().record(new ActivityRecord(
                                action, finalDestJndi, msgId, corrId, priority,
                                props, payload, "SUCCESS", details
                        ));
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error in DynamicAsyncListener onMessage (" + finalDestJndi + ")", e);
                    }
                }
            });

            conn.start();

            ActiveListenerContext context = new ActiveListenerContext(
                    cfJndi, destJndi, destType, selector,
                    durable && isTopic, effectiveClientId, effectiveSubName,
                    System.currentTimeMillis(), conn, session, consumer
            );
            activeListeners.put(destJndi, context);

            String roleName = isTopic
                    ? (isDurableSub ? "Dynamic Durable Topic Subscriber" : "Dynamic Topic Subscriber")
                    : "Dynamic Background Consumer";
            String startDetails = "Started " + roleName + " on " + destJndi
                    + (isDurableSub ? " [ClientID: " + effectiveClientId + ", SubName: " + effectiveSubName + "]" : "")
                    + (selector != null && !selector.trim().isEmpty() ? " [Selector: " + selector.trim() + "]" : "");

            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "ASYNC_START", destJndi, "N/A", "", 0, null, "", "SUCCESS", startDetails
            ));

        } catch (Exception e) {
            if (consumer != null) try { consumer.close(); } catch (Exception ignored) {}
            if (session != null) try { session.close(); } catch (Exception ignored) {}
            if (conn != null) markConnectionDirtyAndClose(conn);

            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "ASYNC_START", destJndi, "N/A", "", 0, null, "", "ERROR",
                    "Failed to start listener on " + destJndi + ": " + e.getMessage()
            ));
            throw e;
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Unsubscribe a durable subscription from JEUS.
     * The subscriber must be closed before unsubscribing.
     */
    public synchronized void unsubscribeDurable(String cfJndi, String clientId, String subscriptionName) throws Exception {
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("ClientID is required to unsubscribe.");
        }
        if (subscriptionName == null || subscriptionName.trim().isEmpty()) {
            throw new IllegalArgumentException("SubscriptionName is required to unsubscribe.");
        }
        final String effectiveClientId = clientId.trim();
        final String effectiveSubName = subscriptionName.trim();

        // Close any active consumer that is currently using this subscription
        for (Map.Entry<String, ActiveListenerContext> entry : new HashSet<>(activeListeners.entrySet())) {
            ActiveListenerContext ctx = entry.getValue();
            if (ctx.isDurable() && effectiveClientId.equals(ctx.getClientId()) && effectiveSubName.equals(ctx.getSubscriptionName())) {
                stop(entry.getKey());
            }
        }

        InitialContext ctx = null;
        Connection conn = null;
        Session session = null;
        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);
            conn = createConnectionWithClientId(cf, effectiveClientId);
            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe(effectiveSubName);

            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "UNSUBSCRIBE", "Topic", "N/A", "", 0, null, "", "SUCCESS",
                    "Deregistered durable subscription '" + effectiveSubName + "' (ClientID: " + effectiveClientId + ") from JEUS"
            ));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to unsubscribe durable subscription: " + effectiveSubName, e);
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "UNSUBSCRIBE", "Topic", "N/A", "", 0, null, "", "ERROR",
                    "Failed to unsubscribe '" + effectiveSubName + "': " + e.getMessage()
            ));
            throw e;
        } finally {
            if (session != null) try { session.close(); } catch (Exception ignored) {}
            if (conn != null) markConnectionDirtyAndClose(conn);
            if (ctx != null) try { ctx.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Closes the connection and marks it dirty if it's a JEUS pooled connection
     * so that it is evicted from the connection pool instead of recycled in an unusable state.
     */
    private static void markConnectionDirtyAndClose(Connection conn) {
        if (conn == null) return;
        try {
            try {
                java.lang.reflect.Method setDirtyMethod = conn.getClass().getMethod("setDirty");
                setDirtyMethod.invoke(conn);
            } catch (Throwable ignored) {
            }
            conn.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Create a Connection with the required ClientID for Durable Subscriptions.
     * Safely handles JEUS connection pool artifacts where recycled connections may have registerable=false,
     * or where the previous connection is in the middle of broker deregistration.
     */
    private Connection createConnectionWithClientId(ConnectionFactory cf, String effectiveClientId) throws Exception {
        Connection conn = null;
        Exception lastException = null;

        for (int attempt = 1; attempt <= 5; attempt++) {
            conn = cf.createConnection();
            String currentClientId = conn.getClientID();

            // If the connection already has the exact ClientID (e.g. assigned by ConnectionFactory config)
            if (effectiveClientId.equals(currentClientId)) {
                LOGGER.log(Level.FINE, "Connection already has matching ClientID: " + effectiveClientId);
                return conn;
            }

            try {
                conn.setClientID(effectiveClientId);
                if (effectiveClientId.equals(conn.getClientID())) {
                    return conn;
                }
            } catch (javax.jms.IllegalStateException ise) {
                lastException = ise;
                LOGGER.log(Level.WARNING, "Connection from factory rejected setClientID (attempt " + attempt + "): "
                        + ise.getMessage() + ". Discarding stale pooled connection and retrying.");
                markConnectionDirtyAndClose(conn);
                conn = null;
            } catch (javax.jms.InvalidClientIDException icid) {
                lastException = icid;
                LOGGER.log(Level.WARNING, "ClientID '" + effectiveClientId + "' is still registered in broker (attempt " + attempt + "): "
                        + icid.getMessage() + ". Waiting for broker deregistration...");
                markConnectionDirtyAndClose(conn);
                conn = null;
                try {
                    Thread.sleep(150);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                lastException = e;
                markConnectionDirtyAndClose(conn);
                conn = null;
                throw e;
            }
        }

        if (conn != null && effectiveClientId.equals(conn.getClientID())) {
            return conn;
        }

        throw new IllegalStateException("Failed to obtain a JMS Connection with ClientID '" + effectiveClientId
                + "' after 5 attempts. Last error: " + (lastException != null ? lastException.getMessage() : "Unknown"), lastException);
    }

    /**
     * Stop the listener on a specific destination.
     */
    public synchronized void stop(String destJndi) {
        if (destJndi == null) return;
        destJndi = destJndi.trim();
        ActiveListenerContext ctx = activeListeners.remove(destJndi);
        if (ctx != null) {
            ctx.close();
            String roleName = "TOPIC".equals(ctx.getDestType())
                    ? (ctx.isDurable() ? "Dynamic Durable Topic Subscriber" : "Dynamic Topic Subscriber")
                    : "Dynamic Background Consumer";
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "ASYNC_STOP", destJndi, "N/A", "", 0, null, "", "SUCCESS",
                    "Stopped " + roleName + " on " + destJndi
            ));
        }
    }

    /**
     * Stop all active listeners and clean up JMS resources.
     */
    public synchronized void stop() {
        for (String dest : new HashSet<>(activeListeners.keySet())) {
            stop(dest);
        }
    }
}
