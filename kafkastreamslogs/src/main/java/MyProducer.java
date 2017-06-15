/**
 * Created by mars137 on 6/8/17.
 */
import java.util.List;
import util. CSVReader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


public class MyProducer
{


    public static void produce(String topic, String fileName, Producer<String, String> prod) {
        //Reading from CSV files
        List<String> lines = CSVReader.getLines(fileName);

        for (String line : lines)
        {
            // Getting the user-id (Cookie-id as the key)  
            String key = line.split(",")[1];

            prod.send(new ProducerRecord<String, String>(topic, key, line), new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception e) {

                    if (e != null) {

                        e.printStackTrace();

                    }

                    System.out.println("Sent for Cookie id:," + key + " topic:," + metadata.topic() + ", Partition: " + metadata.partition());

                }

            });

        };
      prod.close();
    }
}

