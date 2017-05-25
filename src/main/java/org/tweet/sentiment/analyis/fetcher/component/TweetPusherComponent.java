package org.tweet.sentiment.analyis.fetcher.component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class TweetPusherComponent implements Runnable {

    private static final Logger logger = Logger.getLogger(TweetPusherComponent.class.getName());

    private       boolean               isInterrupted;
    private final BlockingQueue<String> incomingQueue;
    private       String                outgoingQueueUrl;
    private       AmazonSQS             outgoingQueue;
    private final String                fetcherTerm;

    public TweetPusherComponent(final BlockingQueue<String> incomingQueue, final AmazonSQS outgoingQueue, final String outgoingQueueUrl, final String fetcherTerm) {
        this.isInterrupted = false;
        this.incomingQueue = incomingQueue;
        this.outgoingQueue = outgoingQueue;
        this.outgoingQueueUrl = outgoingQueueUrl;
        this.fetcherTerm = fetcherTerm;
    }

    @Override
    public void run() {
        while (! this.isInterrupted) {
            try {
                String tweet = this.incomingQueue.poll();

                if (null == tweet) {
                    // no need to send a request to SNS
                    Thread.sleep(200);
                    continue;
                }

                JSONParser parser = new JSONParser();

                try {
                    JSONObject tweetObj = (JSONObject) parser.parse(tweet);

                    Object id = tweetObj.get("id");
                    if (null == id) {
                        logger.warning("No id in tweet. Skipping...");
                        continue;
                    }

                    long tweetId = (long) id;

                    JSONObject wrapperObj = new JSONObject();
                    wrapperObj.put("id", Long.valueOf(tweetId));
                    wrapperObj.put("timestamp", System.currentTimeMillis());
                    wrapperObj.put("tweet", tweetObj);
                    wrapperObj.put("term", this.fetcherTerm);

                    String msgBody = wrapperObj.toJSONString();

                    this.outgoingQueue.sendMessage(this.outgoingQueueUrl, msgBody);

                } catch (ParseException e) {
                    logger.warning("Failed to parse tweet: " + e.getMessage());
                } catch (AmazonServiceException ase) {
                    this.isInterrupted = true;
                    logger.severe("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon SQS, but was rejected with an error response for some reason.");
                    logger.severe("Error Message:    " + ase.getMessage());
                    logger.severe("HTTP Status Code: " + ase.getStatusCode());
                    logger.severe("AWS Error Code:   " + ase.getErrorCode());
                    logger.severe("Error Type:       " + ase.getErrorType());
                    logger.severe("Request ID:       " + ase.getRequestId());
                } catch (AmazonClientException ace) {
                    this.isInterrupted = true;
                    logger.severe("Caught an AmazonClientException, which means the client encountered " +
                            "a serious internal problem while trying to communicate with SQS, such as not " +
                            "being able to access the network.");
                    logger.severe("Error Message: " + ace.getMessage());
                }

            } catch (InterruptedException e) {
                this.isInterrupted = true;
            } catch (Exception e) {
                this.isInterrupted = true;
                logger.severe("Got an error while pushing data to SQS: " + e.getMessage() + " (" + e.toString() + ")");
            }
        }
    }
}
