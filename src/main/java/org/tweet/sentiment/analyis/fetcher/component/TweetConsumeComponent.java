package org.tweet.sentiment.analyis.fetcher.component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class TweetConsumeComponent extends Thread {

    public static final String TWITTER_CONSUMER_KEY    = "TWITTER_CONSUMER_KEY";
    public static final String TWITTER_CONSUMER_SECRET = "TWITTER_CONSUMER_SECRET";
    public static final String TWITTER_TOKEN           = "TWITTER_TOKEN";
    public static final String TWITTER_TOKEN_SECRET    = "TWITTER_TOKEN_SECRET";
    public static final String SQS_QUEUE_NAME          = "SQS_QUEUE_NAME";

    private static final Logger logger = Logger.getLogger(TweetConsumeComponent.class.getName());

    private String          term;
    private boolean         isInterrupted;
    private ExecutorService executorService;

    public TweetConsumeComponent(String term) {
        this.term = term;
        this.isInterrupted = false;
        this.executorService = Executors.newFixedThreadPool(1);
    }

    @Override
    public void run() {
        try {
            AWSCredentials credentials;
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
            AmazonSQS sqsQueue = clientBuilder.build();
            String sqsQueueUrl = sqsQueue.getQueueUrl(System.getenv(SQS_QUEUE_NAME)).getQueueUrl();

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

            logger.info("Using credentials: " + consumerKey + ", " + consumerSecret + ", " + token + ", " + tokenSecret);

            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

            ClientBuilder builder = new ClientBuilder()
                    .name("twitter-consumer-client")
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            Client hosebirdClient = builder.build();

            // start to stream the selected term
            hosebirdClient.connect();
            // submit consumer thread which pushes to consumed tweets to SQS
            this.executorService.submit(new TweetPusherComponent(msgQueue, sqsQueue, sqsQueueUrl, this.term));

            while (! hosebirdClient.isDone() && ! this.isInterrupted) {
                Thread.sleep(500);
            }

            // terminate pushing
            if (! hosebirdClient.isDone()) {
                hosebirdClient.stop();
            }
            this.executorService.shutdownNow();

        } catch (InterruptedException e) {
            this.isInterrupted = true;
        } catch (Exception e) {
            this.isInterrupted = true;
            System.err.println("Got error in TweetConsumeComponent: " + e.getMessage());
        }
    }

    public void cancel() {
        this.isInterrupted = true;
    }

}
