package com.nsv.digital.kafka.producer;

import com.nsv.digital.kafka.domain.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(Sender.class);

    @Value("${kafka.topic.avronewtopic}")
    private String avrotopic;

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public void send(User user) {
        LOGGER.info("sending payload='{}' to topic='{}'", user, avrotopic);
        kafkaTemplate.send(avrotopic, user);
    }
}