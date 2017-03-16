package org.tweet.sentiment.analyis.fetcher.service;

import org.tweet.sentiment.analyis.fetcher.component.TweetConsumeComponent;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class RegistrationService {

    private ExecutorService       executorService;
    private TweetConsumeComponent consumeComponent;

    public RegistrationService() {
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void consumeTweets(String term) {
        this.consumeComponent = new TweetConsumeComponent(term);

        this.executorService.submit(consumeComponent);
    }

    public void stopConsuming() {
        this.consumeComponent.cancel();
    }
}
