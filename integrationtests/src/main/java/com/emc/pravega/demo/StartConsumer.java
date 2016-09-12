package com.emc.pravega.demo;

import com.emc.pravega.stream.*;
import com.emc.pravega.stream.impl.*;

import java.util.concurrent.ExecutionException;

public class StartConsumer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String endpoint = "localhost";
        int port = 9090;
        String scope = "Scope1";
        String streamName = "Stream1";
        String testString = "Hello world: ";

        ApiAdmin apiAdmin = new ApiAdmin(endpoint, port);
        ApiProducer apiProducer = new ApiProducer(endpoint, port);
        ApiConsumer apiConsumer = new ApiConsumer(endpoint, port);
        SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(apiAdmin, apiProducer, apiConsumer, scope);

        Stream stream = null;

        try {
            stream = streamManager.createStream(streamName, null);
        }catch (Exception e) {
            stream = streamManager.getStream(streamName);
        }
        // TODO: remove sleep. It ensures pravega host handles createsegment call from controller before we publish.
        Thread.sleep(1000);

//        @Cleanup
        Consumer<String> consumer = ((SingleSegmentStreamImpl)stream).createConsumer(new JavaSerializer<>(), new ConsumerConfig());
        for (int i = 0; i < 10000; i++) {
            System.out.println(consumer.getNextEvent(5000));
        }


    }
}
