package kafka.producer;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;

public class keycontainer {

    //get value from twitterapipushdataclass
    //fix me: update get function
    String [] args = twitterapipushdata.getArgs();
    String json_file = args[1];
    Object o;

    {
        try {
            o = new JSONParser().parse(new FileReader(json_file));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (org.json.simple.parser.ParseException e) {
            e.printStackTrace();
        }
    }
    JSONObject j = (JSONObject) o;
    private final String consumerKey = (String) j.get("consumer_key");
    private final String consumerSecret = (String ) j.get("consumer_secret");
    private final String token = (String) j.get("access_token");
    private final String secret = (String ) j.get("access_token_secret");

    private final String bootstrap_server = "bootstrap.servers";
    private final String broker_ip = args[0];
    private final String key_serializer = "key.serializer";
    private final String key_serializer_key = "org.apache.kafka.common.serialization.StringSerializer";
    private final String value_serializer = "value.serializer";
    private final String value_serializer_key = "org.apache.kafka.common.serialization.StringSerializer";


    public String getConsumerKey() {
        return consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public String getToken() {
        return token;
    }

    public String getSecret() {
        return secret;
    }

    public String getBootstrap_server() {
        return bootstrap_server;
    }

    public String getBroker_ip() {
        return broker_ip;
    }

    public String getKey_serializer() {
        return key_serializer;
    }

    public String getKey_serializer_key() {
        return key_serializer_key;
    }

    public String getValue_serializer() {
        return value_serializer;
    }

    public String getValue_serializer_key() {
        return value_serializer_key;
    }
}
