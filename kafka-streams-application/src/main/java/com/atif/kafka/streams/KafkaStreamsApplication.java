package com.atif.kafka.streams;

import avro.Message.Event;
import avro.Message.Propensity;
import avro.Message.Row;
import com.atif.kafka.Message.*;
import com.google.common.collect.MinMaxPriorityQueue;

import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreamsApplication {
    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();
        Serde<byte[]> bytearraySerde = Serdes.ByteArray();
        EventDeserializer eventDeserializer = new EventDeserializer();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String, byte[]> logStream = kStreamBuilder.stream(stringSerde, bytearraySerde, "events", "conversions");
        KGroupedStream<String, byte[]> logGroupedStream = logStream.groupByKey();

        KTable<String, byte[]> sequenceTable = logGroupedStream.aggregate(
                () -> {
                    try {
                        return (new EventSerializer()).serializeMessage(Event
                                .newBuilder()
                                .setUserid("")
                                .setRows(new ArrayList<Row>())
                                .build());
                    } catch (IOException e) {
                        System.out.println("initializer failed!!");
                        e.printStackTrace();
                        return new byte[0];
                    }
                },
                (k, v, a) -> {
                    try {
                        Event sequence = eventDeserializer.deserializeEvent(a);
                        Event event = eventDeserializer.deserializeEvent(v);
                        sequence.setUserid(k);
                        List<Row> rowlist = new ArrayList<Row>();
                        rowlist.addAll(sequence.getRows());
                        rowlist.addAll(event.getRows());
                        sequence.setRows(rowlist);
                        return new EventSerializer().serializeMessage(sequence);
                    } catch (Exception e) {
                        System.out.println("here2");
                        e.printStackTrace();
                        return v;
                    }
                },
                bytearraySerde,
                "propensity-local"
        );

        KTable<String, byte[]> propensityTable =
                sequenceTable.mapValues(v -> {
                    Event sequence = null;
                    try {
                        sequence = eventDeserializer.deserializeEvent(v);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    MinMaxPriorityQueue<Row> queue =
                            MinMaxPriorityQueue.orderedBy(new RowComparator()).maximumSize(1000).create(sequence.getRows());
                    try {
                        return new SequenceTransform(sequence.getUserid().toString(), queue).conversionProbability();
                    } catch (IOException e) {
                        e.printStackTrace();
                        return v;
                    }
                });

        propensityTable.foreach((k, v) -> printPropensity(k, v));
        propensityTable.to(stringSerde, bytearraySerde, "propensity-topic");

        System.out.println("Starting sequencing.");
        ZkUtils.maybeDeletePath("localhost:2181", "consumers/try-kafka");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        kafkaStreams.start();
        Thread.sleep(1000L);
        kafkaStreams.close();
        System.out.println("Ending sequencing.");
    }

    public static void printEvent(String k, byte[] v) {
        System.out.println("printEvent");
        EventDeserializer eventDeserializer = new EventDeserializer();
        try {
            Event event = eventDeserializer.deserializeEvent(v);
            EventSerializer eventSerializer = new EventSerializer();
            String jsonString = eventSerializer.serializeMessageToJSON(event);
            System.out.println("Key: " + k + ", " + "Value: " + jsonString);
        } catch (Exception e) {
            System.out.println("Key: " + k + ", " + "Exception: " + e.getMessage());
        }
    }

    public static void printPropensity(String k, byte[] v) {
        System.out.println("printPropensity");
        PropensityDeserializer propensityDeserializer = new PropensityDeserializer();
        try {
            Propensity propensity = propensityDeserializer.deserializeEvent(v);
            PropensitySerializer propensitySerializer = new PropensitySerializer();
            String jsonString = propensitySerializer.serializeMessageToJSON(propensity);
            System.out.println("Key: " + k + ", " + "Value: " + jsonString);
        } catch (Exception e) {
            System.out.println("Key: " + k + ", " + "Exception: " + e.getMessage());
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "try-kafka");
        props.put("group.id", "try-kafka");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "try-kafka-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-13-56-74-46.us-west-1.compute.amazonaws.com:9092,ec2-52-52-174-223.us-west-1.compute.amazonaws.com:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimestampExtractor.class);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return props;
    }

}
