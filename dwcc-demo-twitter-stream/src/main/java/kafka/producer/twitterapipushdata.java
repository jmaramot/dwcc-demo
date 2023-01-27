package kafka.producer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.*;

public class twitterapipushdata {
    private static final String topic = "twitter-data-topic";
    private static String[] savedArgs;

    public static void Pushtwittermessage(Producer producer) throws InterruptedException {

        //creating object for kafka.producer.keycontainer.java
        keycontainer consumerkeys = new keycontainer();

        String consumerKey = consumerkeys.getConsumerKey() ;
        String consumerSecret = consumerkeys.getConsumerSecret();
        String token = consumerkeys.getToken();
        String secret = consumerkeys.getSecret();

        System.out.println("consumerKey:\t" + consumerKey);
        System.out.println("consumerSecret:\t" + consumerSecret);
        System.out.println("token:\t" + token);
        System.out.println("secret:\t" + secret);
        ProducerRecord message = null;


        BlockingQueue queue = new LinkedBlockingQueue(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        //add some track terms

        endpoint.trackTerms(Lists.newArrayList("twitterapi","facebook", "covid"));
//        endpoint.trackTerms(Lists.newArrayList("Aldub Nation"));

        Authentication auth = new OAuth1(consumerKey,consumerSecret,token,secret);

        //Create a new BasicClient. By default gzip is enabled

        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        //Establish a connection
        client.connect();

        //Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++){
            try{
                String msg = (String) queue.take();
                message = new ProducerRecord(topic,queue.take());


            }catch (Exception e) {
                e.printStackTrace();
            }
            producer.send(message);
        }
        producer.close();
        client.stop();
    }

    public static String[] getArgs() {
        return savedArgs;
    }

    public static void main(String[] args) {

        //get values from runtime arguments
        //for optimization: use options builder rather than argument setting
        String broker_ip = args[0];
        String properties_file = args[1];
        System.out.println(properties_file);

        //pass arguments to array to make it accessible to outside classes
        savedArgs = args;

        keycontainer consumerkeys = new keycontainer();
        Properties properties = new Properties();
        //connection to kafka server
        properties.put(consumerkeys.getBootstrap_server(),consumerkeys.getBroker_ip());
//        properties.put(consumerkeys.getBootstrap_server(),broker_ip);
        //Sending string using key serializer
        properties.put(consumerkeys.getKey_serializer(), consumerkeys.getKey_serializer_key());
        //Sending string using value serializer
        properties.put(consumerkeys.getValue_serializer(), consumerkeys.getValue_serializer_key());

        //initialize kafka producer using the properties below
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        try{
            twitterapipushdata.Pushtwittermessage(kafkaProducer);
        }catch (Exception e){
            System.out.println(e);
        }

    }

}
