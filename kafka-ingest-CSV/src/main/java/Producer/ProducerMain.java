import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import util.MyProperties;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by mars137 on 6/11/17.
 */
public class ProducerMain

{

    public static void main(String[] args)
    {
        MyProperties clickProperties = new MyProperties();
        clickProperties.loadProperties("src/main/resources/myproducer_clicks.properties");
        Producer<String, String> prod1 = new KafkaProducer<>(clickProperties);

        MyProperties impressionProperties = new MyProperties();
        impressionProperties.loadProperties("src/main/resources/myproducer_im.properties");
        Producer<String, String> prod2 = new KafkaProducer<>(impressionProperties);

        MyProperties conversionProperties = new MyProperties();
        conversionProperties.loadProperties("src/main/resources/myproducer_st.properties");
        try (Producer<String, String> prod3 = new KafkaProducer<>(conversionProperties)) {


            Properties fileProperties = new Properties();
            fileProperties.load(ProducerMain.class.getResourceAsStream("/file_config.properties"));

            String clicks_path = fileProperties.getProperty("clicks_path");
            String impression_path = fileProperties.getProperty("impression_path");
            String site_path = fileProperties.getProperty("site_clicks");

            String topic_clicks = fileProperties.getProperty("topic_clicks");
            String topic_impressions = fileProperties.getProperty("topic_impressions");
            String topic_site = fileProperties.getProperty("topic_site");

            MyProducer pr1 = new MyProducer();
            MyProducer pr2 = new MyProducer();
            MyProducer pr3 = new MyProducer();

            pr1.produce(topic_clicks, clicks_path, prod1);
            pr2.produce(topic_impressions, impression_path, prod2);
            pr3.produce(topic_site, site_path, prod3);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

















}






