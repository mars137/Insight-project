import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import util.MyProperties;

/**
 * Created by mars137 on 6/11/17.
   Producer executor for generating kafka streams for Ad clicks, impressions and site_analytics(conversions)
 */
public class ProducerMain

{

    public static void main(String[] args)
    {
        
        //Properties for initializing the Kafka producer
        MyProperties myProperties1 = new MyProperties();
        myProperties1.loadProperties("src/main/resources/myproducer_clicks.properties");
        Producer<String, String> prod1 = new KafkaProducer<>(myProperties1);

        MyProperties myProperties2 = new MyProperties();
        myProperties2.loadProperties("src/main/resources/myproducer_im.properties");
        Producer<String, String> prod2 = new KafkaProducer<>(myProperties2);

        MyProperties myProperties3 = new MyProperties();
        myProperties3.loadProperties("src/main/resources/myproducer_st.properties");
        Producer<String, String> prod3 = new KafkaProducer<>(myProperties3);


        //CSV files downloaded from S3 
        String clicks_path = "/home/hadoop-user/Data_sets/clicks_1.csv";
        String impression_path = "/home/hadoop-user/Data_sets/im.csv";
        String site_analytics = "/home/hadoop-user/Data_sets/ste.csv";
        //Topics for kafka producer
        String topic_clicks= "cltest";
        String topic_impressions ="imtest";
        String topic_site="sitecl";
        
        //Intitalizing object of MyProducer class containing the code for producer implementation
        MyProducer pr1 = new MyProducer();
        MyProducer pr2 = new MyProducer();
        MyProducer pr3 = new MyProducer();
        
        //Start the Kafka producer using the above mentioned topics 
        pr1.produce(topic_clicks,clicks_path,prod1);
        pr2.produce(topic_impressions,impression_path,prod2);
        pr3.produce(topic_site,site_analytics,prod3);

    }
}
