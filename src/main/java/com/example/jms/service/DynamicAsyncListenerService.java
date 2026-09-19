package com.example.jms.service;

import com.example.jms.model.ActivityRecord;
import com.example.jms.model.MessageActivityStore;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages an on-demand, web-controlled asynchronous JMS MessageListener.
 * Allows testing asynchronous message reception on any JNDI queue dynamically,
 * without needing an EJB MDB or fixed deployment descriptors.
 */
public class DynamicAsyncListenerService {

    private static final Logger LOGGER = Logger.getLogger(DynamicAsyncListenerService.class.getName());
    private static final DynamicAsyncListenerService INSTANCE = new DynamicAsyncListenerService();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private String currentDestination = null;
    private Connection activeConnection = null;
    private Session activeSession = null;
    private MessageConsumer activeConsumer = null;

    private DynamicAsyncListenerService() {}

    public static DynamicAsyncListenerService getInstance() {
        return INSTANCE;
    }

    public synchronized boolean isRunning() {
        return running.get();
    }

    public synchronized String getCurrentDestination() {
        return currentDestination;
    }

    /**
     * Start listening asynchronously on the given Queue.
     */
    public synchronized void start(String cfJndi, String queueJndi) throws Exception {
        if (running.get()) {
            stop();
        }

        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(cfJndi);
            Queue queue = (Queue) ctx.lookup(queueJndi);

            activeConnection = cf.createConnection();
            activeSession = activeConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            activeConsumer = activeSession.createConsumer(queue);

            activeConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        String msgId = message.getJMSMessageID();
                        String corrId = message.getJMSCorrelationID();
                        int priority = message.getJMSPriority();
                        Map<String, String> props = JmsService.extractProperties(message);
                        String payload = JmsService.extractPayload(message);

                        MessageActivityStore.getInstance().record(new ActivityRecord(
                                "ASYNC_RECV", queueJndi, msgId, corrId, priority,
                                props, payload, "SUCCESS",
                                "Received asynchronously by Dynamic Background Listener"
                        ));
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error in DynamicAsyncListener onMessage", e);
                    }
                }
            });

            activeConnection.start();
            running.set(true);
            currentDestination = queueJndi;

            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "ASYNC_START", queueJndi, "N/A", "", 0, null, "", "SUCCESS",
                    "Started Dynamic Background Listener on " + queueJndi
            ));

        } catch (Exception e) {
            stop();
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "ASYNC_START", queueJndi, "N/A", "", 0, null, "", "ERROR",
                    "Failed to start Dynamic Listener: " + e.getMessage()
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
     * Stop the active listener and clean up JMS resources.
     */
    public synchronized void stop() {
        String lastDest = currentDestination;
        running.set(false);
        currentDestination = null;

        if (activeConsumer != null) {
            try {
                activeConsumer.close();
            } catch (Exception ignored) {}
            activeConsumer = null;
        }
        if (activeSession != null) {
            try {
                activeSession.close();
            } catch (Exception ignored) {}
            activeSession = null;
        }
        if (activeConnection != null) {
            try {
                activeConnection.close();
            } catch (Exception ignored) {}
            activeConnection = null;
        }

        if (lastDest != null) {
            MessageActivityStore.getInstance().record(new ActivityRecord(
                    "ASYNC_STOP", lastDest, "N/A", "", 0, null, "", "SUCCESS",
                    "Stopped Dynamic Background Listener"
            ));
        }
    }
}
