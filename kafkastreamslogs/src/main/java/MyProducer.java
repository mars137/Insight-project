/**
 * Created by mars137 on 6/8/17.
 */
import java.util.ArrayList;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.MyCsv;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


public class MyProducer
{


    public static void produce(String topic, String fileName,Producer prod)
      {

        Map<String, ArrayList<String>> map = MyCsv.readAndMap(fileName, 1);



        try {
            map.forEach((key, value) -> {
                value.forEach(sub -> {
                    prod.send(new ProducerRecord<String, String>(topic, key, sub), new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            }
                            System.out.println("Sent for Cookie id: " + key + ", Partition: " + metadata.partition());

                        }
                    });
                });
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


                // closes producer
            });
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            prod.close();
        }

    }
}

