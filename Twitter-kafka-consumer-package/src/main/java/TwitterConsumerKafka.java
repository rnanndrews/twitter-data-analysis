//**********************************************************************************//
// *                    TWITTER CONSUMER KAFKA                                    * //
// *  Date:25-Sep-2021                                                            * //
// *  Description: This package retrieves the tweets from broker and insert into  * //
// *               MongoDB                                                        * //
//**********************************************************************************//
// *                        VERSION DETAILS                                       * //
// *  Version 1: Base version of twitter kafka consumer which retrieves tweets    * //
// *             from broker and insert into MongoDB                              * //
//**********************************************************************************//
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumerKafka
{
    Logger lLog = LoggerFactory.getLogger(TwitterConsumerKafka.class.getName());

    public TwitterConsumerKafka()
    {

    }
    public static void main(String[] args)
    {
        new TwitterConsumerKafka().startConsumer();
    }

    public void startConsumer()
    {
        lLog.info("Process Started");

        KafkaConsumer<String, String> consumer = startKafkaConsumer("Tweets_Topic");
        lLog.info("Kafka Consumer is setup");

        //Creating a MongoDB client
        MongoClient mongo = new MongoClient( "localhost" , 27017 );
        //Connecting to the database
        MongoDatabase database = mongo.getDatabase("Twitter");
        lLog.info("Mongo DB Client is setup");

        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                //Insert into MongoDB
                String sTweetId = extractValueFromTweet(record.value(), "id_str");
                String sCreatedAt = extractValueFromTweet(record.value(), "created_at");
                String sTweet = extractValueFromTweet(record.value(), "text");
                String sLocation = extractLocationFromTweet(record.value());

                //Preparing a document
                Document dDocument = new Document();
                dDocument.append("tweetId", sTweetId);
                dDocument.append("created_at", sCreatedAt);
                dDocument.append("tweet", sTweet);
                dDocument.append("location", sLocation);
                //Inserting the document into the collection
                database.getCollection("tweets").insertOne(dDocument);
                lLog.info("Record is:"+record.value()+":RecordEndHere");
            }
        }
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractValueFromTweet(String sTweetJson, String sTag)
    {
        String sValue;
        try
        {
            sValue= jsonParser.parse(sTweetJson)
                    .getAsJsonObject()
                    .get(sTag)
                    .getAsString();
        }catch (Exception Ex) {
            sValue = "";
        }
        return sValue;
    }

    private static String extractLocationFromTweet(String sTweetJson)
    {
       String sLocation="";
       try
       {
            sLocation= jsonParser.parse(sTweetJson)
                        .getAsJsonObject()
                        .get("user")
                        .getAsJsonObject()
                        .get("location")
                        .getAsString();

       }catch (Exception Ex) {
                sLocation = "";
       }
       return sLocation;
    }

    public static KafkaConsumer<String, String> startKafkaConsumer(String topic)
    {

        String sBootstrapServers = "127.0.0.1:9092";
        String sGroupId = "kafka_tweet_group";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sBootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, sGroupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

}
