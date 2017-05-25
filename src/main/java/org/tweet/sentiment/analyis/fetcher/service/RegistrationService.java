package org.tweet.sentiment.analyis.fetcher.service;

import org.springframework.stereotype.Service;
import org.tweet.sentiment.analyis.fetcher.component.TweetConsumeComponent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Service
public class RegistrationService {

    private static final Logger logger = Logger.getLogger(RegistrationService.class.getName());

    private static final int CONSUME_TWEET_THREADS = 10;

    private ExecutorService                    executorService;
    private Map<String, TweetConsumeComponent> registeredTerms;

    public RegistrationService() {
        this.executorService = Executors.newFixedThreadPool(CONSUME_TWEET_THREADS);
        this.registeredTerms = new HashMap<>();
    }

    public void consumeTweets(String term) {
        logger.info("Starting to consume tweets for term " + term);
        TweetConsumeComponent consumeComponent = new TweetConsumeComponent(term);
        this.registeredTerms.put(term, consumeComponent);
        this.executorService.submit(consumeComponent);
    }

    public void stopConsuming(String term) {
        if (this.registeredTerms.containsKey(term)) {
            logger.info("Stopping to consume tweets for term " + term);
            this.registeredTerms.remove(term).cancel();
        }
    }
}
