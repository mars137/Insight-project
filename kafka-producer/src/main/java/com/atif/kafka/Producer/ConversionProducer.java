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
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("producer_convert.bootstrap,servers"));
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
        Long startTime = millisSinceEpochFrom("2017-06-15 02:10:20.000 UTC");
        int k = 0;
        int productIndex = 0;
        int stateIdx = 0;
        int segmentIdx = 0;
        
        // Generate one week worth of data
        for (long ts = startTime; ts <= startTime + 7*24*60*60*1000; ts = ts + 24*60*60*1000) {
        	for (int u = 1; u <= 400; u++) {
        		if (stateIdx >= 50) 
        			stateIdx = 0;
   			
        		if (segmentIdx >= 3)
        			segmentIdx = 0;
        	
        		// Set user demographics information
        		String userSegment = UserProperties.getSegmentList()[segmentIdx++];
        		String userState =UserProperties.getStateList()[stateIdx++];
        	
        		if (u%10 != 0)
        			continue;
        	
        		if (u % 100 == 0) {
        			k = 2;
        		} else if (u % 90 == 0 ) {
        			k = 3;
        		} else if (u % 80 == 0) {
        			k = 5;
        		} else if (u % 70 == 0) {
        			k = 7;
        		} else {
        			k = 10;
        		}
       		       			
        		String userid = "u" + u;
        	
        		for (int i = 1; i <= k; i++) {
        			if (productIndex >= 3)
            		productIndex = 0;
            	
        			Row row = Row
        				.newBuilder()
        				.setUuid(UUID.randomUUID().toString())
        				.setTimestamp(ts)
        				.setUserid(userid)
        				.setCampaign("fall sale")
        				.setLogtype("CN")
        				.setProduct(products[productIndex++])
        				.setQuantity(1)
        				.build();
        		
        			try {
        				EventSerializer eventSerializer = new EventSerializer();
        				
        				Event event = Event
        					.newBuilder()
        					.setUserid(userid)
        					.setState(userState)
        					.setSegment(userSegment)
        					.setRows(Arrays.asList(row))
        					.build();
        				
        				sp.publish("conversions", ts, event.getUserid().toString(), eventSerializer.serializeMessage(event));
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
    }
}
