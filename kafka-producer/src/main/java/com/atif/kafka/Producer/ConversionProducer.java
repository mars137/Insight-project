package com.atif.kafka.producer;

import avro.Message.Event;
import avro.Message.Row;
import com.atif.kafka.Message.EventSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConversionProducer {
    private final Producer<String, byte[]> kafkaProducer;
    private static final Logger logger = LoggerFactory.getLogger(MarketingProducer.class);
    private static final Config config = ConfigFactory.load("producer_convert");

    public ConversionProducer() {
        logger.debug("added props");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("producer_convert.bootstrap.servers"));
        props.put(ProducerConfig.ACKS_CONFIG, config.getString("producer_convert.acks"));
        props.put(ProducerConfig.RETRIES_CONFIG, config.getString("producer_convert.retries"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString("producer_convert.compression_type_config"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getString("producer_convert.batch.size"));
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getString("producer_convert.linger.ms"));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getString("producer_convert.buffer.memory"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getString("producer_convert.key.serializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getString("producer_convert.value.serializer"));
        kafkaProducer = new KafkaProducer<String, byte[]>(props);
    }

    public void publish(String topic, Long ts, String userid, byte[] event) throws ExecutionException, InterruptedException {
        logger.debug("Send message");
        RecordMetadata m = kafkaProducer.send(new ProducerRecord<String, byte[]>(topic, null, ts, userid, event)).get();
        System.out.println("Message produced, offset: " + m.offset());
        System.out.println("Message produced, partition : " + m.partition());
        System.out.println("Message produced, topic: " + m.topic());
    }

    public static Long millisSinceEpochFrom(String ts) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS z").parse(ts).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("TimeStamp parsing exception. Setting timestamp to 0L.");
            return 0L;
        }
    }
    public static void main(String[] args) {
        MarketingProducer sp = new MarketingProducer();
        String[] products ={"Hat", "Shoes", "Bag"};
        int productIndex = 0;
        
        Long ts = millisSinceEpochFrom("2017-06-06 00:00:00.000 UTC");
        for (int u = 1; u <= 400; u++) {
        	if (u%10 != 0)
        		continue;
        	String userid = "u" + u;
        	if (productIndex >= 3)
        		productIndex = 0;
        	Row row = Row
                .newBuilder()
                .setUuid(UUID.randomUUID().toString())
                .setTimestamp(ts)
                .setUserid(userid)
                .setCampaign("fall sale")
                .setLogtype("CONVERSION")
                .setProduct(products[productIndex++])
                .setQuantity(1)
                .build();

        	Event event = Event.newBuilder().setUserid(userid).setRows(Arrays.asList(row)).build();
        	try {
        		EventSerializer eventSerializer = new EventSerializer();
        		for (int i = 0; i < 2; i++) {
        			sp.publish("conversions", ts, event.getUserid().toString(), eventSerializer.serializeMessage(event));
        		}
        	} catch (EOFException e) {
        		e.printStackTrace();
        	} catch (IOException e) {
        		e.printStackTrace();
        	} catch (InterruptedException e) {
        		e.printStackTrace();
        	} catch (ExecutionException e) {
        		e.printStackTrace();
        	}
        }
    }
}

