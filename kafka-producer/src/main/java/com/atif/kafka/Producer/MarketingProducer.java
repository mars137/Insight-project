package com.atif.kafka.producer;

import avro.Message.Event;
import avro.Message.Row;
import com.atif.kafka.Message.EventSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.EOFException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MarketingProducer {
    private final Producer<String, byte[]> kafkaProducer;
    private static final Logger logger = LoggerFactory.getLogger(MarketingProducer.class);
	private static final Config config = ConfigFactory.load("marketing");

	public MarketingProducer() {
		logger.debug("added props");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("marketing.bootstrap.servers"));
		props.put(ProducerConfig.ACKS_CONFIG, config.getString("marketing.acks"));
		props.put(ProducerConfig.RETRIES_CONFIG, config.getString("marketing.retries"));
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString("marketing.compression.type.config"));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getString("marketing.batch.size"));
		props.put(ProducerConfig.LINGER_MS_CONFIG, config.getString("marketing.linger.ms"));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getString("marketing.buffer.memory"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getString("marketing.key.serializer"));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getString("marketing.value.serializer"));
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
        String[] marketingType = {"IM", "CL", "PS"};
        int marketingIdx = 0;
        int stateIdx = 0;
        int segmentIdx = 0;
        Long startTime = millisSinceEpochFrom("2017-06-15 00:00:00.000 UTC");
        int k = 10;
        
        // Generate one week worth of data
        for (long ts = startTime; ts <= startTime + 7*24*60*60*1000; ts = ts + 24*60*60*1000) {
        	for (int u = 1; u <= 400; u++) {
        		String userid = "u" + u;
        		if (stateIdx >= 50) 
        			stateIdx = 0;
        		if (segmentIdx >= 3)
        			segmentIdx = 0;

        		// Set user demographics information
        		String userSegment = UserProperties.getSegmentList()[segmentIdx++];
        		String userState =UserProperties.getStateList()[stateIdx++];
        	
        		try {
        			EventSerializer eventSerializer = new EventSerializer();
        			if (u % 2 == 0) {
        				k = 2;
        			} else if (u % 3 == 0 ) {
        				k = 3;
        			} else if (u % 5 == 0) {
        				k = 5;
        			} else if (u % 7 == 0) {
        				k = 7;
        			} else {
        				k = 10;
        			}
					for (int i = 0; i < k; i++) {
						String type;
						double rand = Math.random();
						if(rand >= 0.90) {
							type = "IM";
						} else if (rand >= 0.08) {
							type = "CL";
						}
						else {
							type = "PS";
						}

						Row row = Row
								.newBuilder()
								.setUuid(UUID.randomUUID().toString())
								.setTimestamp(ts+k+i)
								.setUserid(userid)
								.setCampaign("fall sale")
								.setPublisher("nyt")
								.setCreative("advert.tiff")
								.setLogtype(type)
								.setPlacement("200x400")
								.build();

        	        	Event event = Event
        	        		.newBuilder()
        	        		.setUserid(userid)
        	        		.setState(userState)
        	        		.setSegment(userSegment)
        	        		.setRows(Arrays.asList(row))
        	        		.build();
        	        	
        	        	sp.publish("events", ts, event.getUserid().toString(), eventSerializer.serializeMessage(event));
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
}

