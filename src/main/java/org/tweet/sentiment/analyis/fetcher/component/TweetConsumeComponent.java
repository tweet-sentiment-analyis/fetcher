package org.tweet.sentiment.analyis.fetcher.component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetConsumeComponent extends Thread {

    public static final String TWITTER_CONSUMER_KEY = "TWITTER_CONSUMER_KEY";
    public static final String TWITTER_CONSUMER_SECRET = "TWITTER_CONSUMER_SECRET";
    public static final String TWITTER_TOKEN = "TWITTER_TOKEN";
    public static final String TWITTER_TOKEN_SECRET = "TWITTER_TOKEN_SECRET";

    private String term;

    private boolean isInterrupted;

    private AmazonSQS simpleQueue;
    private String    queueUrl;


    public TweetConsumeComponent(String term) {
        this.term = term;
        this.isInterrupted = false;

        this.init();
    }

    private void init() {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new EnvironmentVariableCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the environment. " +
                            "Please make sure that your credentials are located in the environment variables " +
                            "AWS_ACCESS_KEY_ID resp. AWS_SECRET_ACCESS_KEY",
                    e);
        }

        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
        clientBuilder.setRegion(Regions.US_WEST_2.getName());
        this.simpleQueue = clientBuilder.build();
        this.queueUrl = simpleQueue.getQueueUrl("fetched-tweets").getQueueUrl();
    }

    @Override
    public void run() {
        try {
            // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

            // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

            List<String> terms = Lists.newArrayList(this.term);
            hosebirdEndpoint.trackTerms(terms);

            // read secrets from environment variable
            String consumerKey = System.getenv(TWITTER_CONSUMER_KEY);
            String consumerSecret = System.getenv(TWITTER_CONSUMER_SECRET);
            String token = System.getenv(TWITTER_TOKEN);
            String tokenSecret = System.getenv(TWITTER_TOKEN_SECRET);

            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01") // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));
            //.eventMessageQueue(eventQueue); // optional: use this if you want to process client events

            Client hosebirdClient = builder.build();
            // Attempts to establish a connection.
            hosebirdClient.connect();

            JSONParser parser = new JSONParser();
            try {
                // on a different thread, or multiple different threads....
                while (! hosebirdClient.isDone() && ! this.isInterrupted) {
                    String msg = msgQueue.take();


                    JSONObject tweetObj = (JSONObject) parser.parse(msg);
                    long tweetId = (long) tweetObj.get("id");

                    JSONObject wrapperObj = new JSONObject();
                    wrapperObj.put("id", tweetId);
                    wrapperObj.put("timestamp", System.currentTimeMillis());
                    wrapperObj.put("tweet", tweetObj);
                    wrapperObj.put("term", this.term);

                    String str = wrapperObj.toJSONString();

                    System.out.println(str);

                    sendMessageToSQS(str);
                }

                hosebirdClient.stop();

            } catch (InterruptedException e) {
                hosebirdClient.stop();
                Thread.currentThread().interrupt();
            } catch (ParseException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Got error in TweetConsumeComponent: " + e.getMessage());
        }
    }

    public void cancel() {
        this.isInterrupted = true;
    }

    private void sendMessageToSQS(String msg) {
        try {
            // Send a message
            SendMessageResult result = this.simpleQueue.sendMessage(new SendMessageRequest(this.queueUrl, msg));

        } catch (AmazonServiceException ase) {
            System.err.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.err.println("Error Message:    " + ase.getMessage());
            System.err.println("HTTP Status Code: " + ase.getStatusCode());
            System.err.println("AWS Error Code:   " + ase.getErrorCode());
            System.err.println("Error Type:       " + ase.getErrorType());
            System.err.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.err.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.err.println("Error Message: " + ace.getMessage());
        }
    }
}
