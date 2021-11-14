//**********************************************************************************//
// *                    TWITTER PRODUCER KAFKA                                    * //
// *  Date:25-Sep-2021                                                            * //
// *  Description: This package uses the Twitter API to retrieve the tweets and   * //
// *               send it to Kafka broker                                        * //
// *  Input Parameters: 1. Topic Name                                             * //
// *                    2. List of Tags for twitter                               * //
// *  Sample: Tweets_topic covid corona pfizer moderna                            * //
//**********************************************************************************//
// *                        VERSION DETAILS                                       * //
// *  Version 1: Base version of twitter kafka producer which retrieves tweets    * //
// *             and pass it to kafka                                             * //
//**********************************************************************************//
// *  Version 2: Dynamic processing of topic and group. This enables the consumer * //
// *             to be launched in multiple instance with differnt configuration  * //
//**********************************************************************************//

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerKafka
{
    Logger lLog = LoggerFactory.getLogger(TwitterProducerKafka.class.getName());

    // Setting up the keys for Twitter API***** THESE KEYS ARE GENERATE ON TWITTER DEVELOPER ACCOUNT FOR THE API
    String sAPIConsumerKey = ConsumerKey;
    String sAPIConsumerSecret = ConsumerSecret;
    String sAPIToken = APIToken;
    String sAPISecret = APISecret;

    //List of tags to search and retrieve tweets
   // List<String> lTweetTags = Lists.newArrayList("Pfizer","BioNTech","Moderna","JohnsonandJohnson"
   //                                                      ,"JNJNews","Janssen","Corona","Covid"
   //                                                      ,"vaccination","vaccine","antivaccine","antivax");

    public TwitterProducerKafka()
    {

    }

    public static void main(String[] args)
    {
        List<String> lDynTags = Lists.newArrayList(Arrays.asList(args));
        String sTopicName;

        if(lDynTags.size() < 2)
        {
            System.out.println("Please provide topic name and tags. Atleast 2 parameters");
        }
        else
        {
            sTopicName = lDynTags.get(0);
            System.out.println("Topic Name:"+sTopicName);
            lDynTags.remove(0);
            System.out.println("Twitter Tags:"+lDynTags);
            new TwitterProducerKafka().startProducer(sTopicName, lDynTags);
        }

    }

    public void startProducer(String sTopicName, List lTweetTags)
    {
        lLog.info("Process Started: Topic:"+sTopicName);

        /** Setting up blocking queues*/
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create the twitter client
        Client client = createTwitterClient(msgQueue, lTweetTags);
        lLog.info("Twitter Client setup is completed");

        // Establish connection with API.
        client.connect();
        lLog.info("Twitter Client is connected");

        // create a kafka producer
        KafkaProducer<String, String> producer = setupKafkaProducer();
        lLog.info("Kafka producer is setup");

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            lLog.info("Shutting down client from twitter.");
            client.stop();
            lLog.info("Twitter Client shut down.");
            lLog.info("Closing producer");
            producer.close();
            lLog.info("Producer close and process shutdown.");
        }));

        // Get the tweets and push it via producer
        while (!client.isDone())
        {
            String msg = null;
            try
            {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null)
            {
                lLog.info(msg);
                producer.send(new ProducerRecord<>(sTopicName, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e)
                    {
                        if (e != null)
                        {
                            lLog.error("Error in tweets processing", e);
                        }
                    }
                });
            }
        }
        lLog.info("Application shutdown");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue, List lTweetTags)
    {

        Hosts hHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint sfEndpoint = new StatusesFilterEndpoint();

        sfEndpoint.trackTerms(lTweetTags);

        // Passing the secret config for API
        Authentication hAuth = new OAuth1(sAPIConsumerKey,
                                          sAPIConsumerSecret,
                                          sAPIToken,
                                          sAPISecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-API-Annd")
                .hosts(hHosts)
                .authentication(hAuth)
                .endpoint(sfEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hClient = builder.build();
        return hClient;
    }

    public KafkaProducer<String, String> setupKafkaProducer()
    {
        String sBootstrapServers = "127.0.0.1:9092";

        // Set Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sBootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
