package com.atif.kafka.Message;

import avro.Message.Row;

import java.util.Comparator;

public class RowComparator implements Comparator<Row> {
    @Override
    public int compare(Row t0, Row t1) {
        long l0 = t0.getTimestamp();
        long l1 = t1.getTimestamp();
        return (l0 > l1) ? 1 : (l0 == l1) ? 0 : -1;
    }
}
