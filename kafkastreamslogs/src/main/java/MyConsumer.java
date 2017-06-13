import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by mars137 on 6/10/17.
 */


public class MyConsumer implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);

    public MyConsumer(int id, List<String> topics, Properties consumerProps) {
        this.id = id;
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            System.setProperty("log.name", "test");
            log.info("Consumer {} subscribed ...", id);

            while (true) {
                log.info("Consumer {} polling ...", id);
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                log.info("Received {} records", records.count());

                for (TopicPartition topicPartition : records.partitions()) {
                    List<ConsumerRecord<String, String>> topicRecords = records.records(topicPartition);

                    for (ConsumerRecord<String, String> record : topicRecords) {
                        log.info("ConsumerId:{}-Topic:{} => Partition={}, Offset={}, EventTime:[{}] Val={}", id,
                                topicPartition.topic(), record.partition(), record.offset(), record.timestamp(),
                                record.value());
                    }

                    long lastPartitionOffset = topicRecords.get(topicRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(topicPartition,
                            new OffsetAndMetadata(lastPartitionOffset + 1)));
                }
            }
        } catch (WakeupException ignored) {
            // ignore for shutdown
        } catch (Exception e) {
            log.error("Consumer encountered error", e);
        }

        /*finally {
            consumer.close();
        }*/
    }

    public void shutdown() {
        //trigger a
        consumer.wakeup();
    }
}