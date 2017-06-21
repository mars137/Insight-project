package com.atif.kafka.Message;

import avro.Message.Event;

import java.util.Comparator;

/**
 * Created by mars137 on 6/18/17.
 */
public class EventComparator implements Comparator<Event> {
    @Override
    public int compare(Event t0, Event t1) {
        long l0 = t0.getRows().get(0).getTimestamp();
        long l1 = t1.getRows().get(0).getTimestamp();
        return (l0 > l1) ? 1 : (l0 == l1) ? 0 : -1;
    }
}
