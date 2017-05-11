package com.twitterfeed.producer;

import twitter4j.*;
import java.util.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public final class TwitterKafkaProducer {
    public static void main(String[] args) throws TwitterException {

        //Define properties for how the Producer finds the cluster, serializes 
        //the messages and if appropriate directs the message to a specific 
        //partition.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.twitterfeed.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        //Define producer object, its a java generic and takes 2 params; first
        //type of partition key, second type of the message
        final Producer<String, String> producer = new Producer<>(config);
        
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                String msg = "@" + status.getUser().getScreenName() + " - " + status.getText();
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("tw", msg);
                producer.send(data);
                System.out.println(msg);
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                producer.close();
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }
            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }
            @Override
            public void onException(Exception ex) {
            }
        };
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        ArrayList<Long> follow = new ArrayList<Long>();
        ArrayList<String> track = new ArrayList<String>();
        long[] followArray = new long[follow.size()];
        track.add("Messi");
        String[] trackArray = track.toArray(new String[track.size()]);
        twitterStream.filter(new FilterQuery(trackArray));
    
    }
}
