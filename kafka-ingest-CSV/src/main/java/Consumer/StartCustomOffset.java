/**
 * Created by mars137 on 6/12/17.
 */
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author mars137
 */
public class StartCustomOffset {

    private static final Config config = ConfigFactory.load("consumer");

    private static final String CONSUMER_GROUP_ID = "consumer-processing";

    public static void main(String[] args) {
        int numConsumers = 1;

        List<String> topics = Arrays.asList(config.getString("consumer.topics"));
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);


        final List<ConsumerCustomOffset> consumers = new ArrayList<>();
        for (int i = 1; i <= numConsumers; i++) {
            ConsumerCustomOffset consumer = new ConsumerCustomOffset(i, 2,
                    topics, properties(CONSUMER_GROUP_ID));
            consumers.add(consumer);

            executor.submit(consumer);
        }


    }



    private static Properties properties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("consumer.bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        return props;
    }

}