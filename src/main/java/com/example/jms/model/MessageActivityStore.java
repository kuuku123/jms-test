package com.example.jms.model;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Thread-safe in-memory store for recent JMS activities.
 * Acts as a circular buffer retaining the last 100 events.
 */
public class MessageActivityStore {

    private static final int MAX_ACTIVITIES = 100;
    private static final MessageActivityStore INSTANCE = new MessageActivityStore();

    private final LinkedList<ActivityRecord> activities = new LinkedList<>();

    private MessageActivityStore() {}

    public static MessageActivityStore getInstance() {
        return INSTANCE;
    }

    public synchronized void record(ActivityRecord record) {
        if (activities.size() >= MAX_ACTIVITIES) {
            activities.removeLast();
        }
        activities.addFirst(record);
    }

    public synchronized List<ActivityRecord> getActivities() {
        return new ArrayList<>(activities);
    }

    public synchronized void clear() {
        activities.clear();
    }
}
