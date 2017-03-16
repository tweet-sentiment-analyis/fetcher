package org.tweet.sentiment.analyis.fetcher.component;

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
import java.util.concurrent.LinkedBlockingQueue;

public class TweetConsumeComponent extends Thread {

    private String term;

    private boolean isInterrupted;

    public TweetConsumeComponent(String term) {
        this.term = term;
        this.isInterrupted = false;
    }

    @Override
    public void run() {

        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList(this.term);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("OqJqW85qsdLeahgZ8TRSciCk1", "J3vYnVcdC4beNeaGpY7p2KUjZDpXaa4JMQ5XiWsz3td5NPcTAb", "3467421496-hRoikICJapjithg2r2zpKRNeYQK32FjWVnoB4ta", "TLvUJ1NG00ph1uu6yA45V012CwAR1yZp5pj79xoRv7azL");

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


        try {
            // on a different thread, or multiple different threads....
            while (! hosebirdClient.isDone() && ! this.isInterrupted) {
                String msg = msgQueue.take();
                System.out.println(msg);
            }

            hosebirdClient.stop();

        } catch (InterruptedException e) {
            hosebirdClient.stop();
            Thread.currentThread().interrupt();
        }
    }

    public void cancel() {
        this.isInterrupted = true;
    }
}
