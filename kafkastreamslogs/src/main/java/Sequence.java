import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.examples.pageview.JsonTimestampExtractor;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import util.WindowedSerde;

import java.util.Properties;

/**
 * Created by mars137 on 6/13/17.
 */
public class Sequence


{

    public static void main (String[] args) throws Exception {

        final Properties streamsConfiguration = new Properties();
        final Config config = ConfigFactory.load("sequence");

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sequence");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("sequence.bootstrap.servers"));
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(stringSerde);
        KStreamBuilder builder = new KStreamBuilder();
        //Clicks and impression streams
        KStream<String, String> clim = builder.stream("cltest1","imtest1");

        KTable <String,Long>  strcount = clim.groupByKey().count("Counts");
        strcount.to(Serdes.String(), Serdes.Long(), "cntevent1");


        strcount.toStream().process(() -> new AbstractProcessor<String, Long>() {
                    @Override
                    public void process(String userid, Long cnt) {
                        System.out.println("User" + userid + "Count" + cnt );
                    }
                });

        //executing the stream
        KafkaStreams sequence_streams = new KafkaStreams(builder, streamsConfiguration);
        Long tbeg= System.currentTimeMillis();
        sequence_streams.start();
        Thread.sleep(10000L);
        Long tend = System.currentTimeMillis();
        double evnts = (20000.0 / (tend - tbeg)) * 1000.0;
        System.out.println("Consumed" + evnts + "events and activities per second.");
        //sequence_streams.close();
    }
}