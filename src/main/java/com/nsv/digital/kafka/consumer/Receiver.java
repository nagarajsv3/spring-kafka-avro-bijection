package com.nsv.digital.kafka.consumer;

import com.nsv.digital.kafka.domain.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class Receiver {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(Receiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.avronewtopic}")
    public void receive(User user) {
        LOGGER.info("received payload='{}'", user);
        latch.countDown();
    }

}