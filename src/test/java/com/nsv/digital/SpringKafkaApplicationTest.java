package com.nsv.digital;

import com.nsv.digital.kafka.consumer.Receiver;
import com.nsv.digital.kafka.domain.avro.User;
import com.nsv.digital.kafka.producer.Sender;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Java6Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SpringKafkaApplicationTest {

    @Value("${kafka.topic.avronewtopic}")
    private String C3TOPIC = null;
    //private static final String C3TOPIC = "c3testtopic";

    //Uncomment to use embedded kafka broker
    /*@ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1,true,C3TOPIC);*/

    @Autowired
    private Receiver receiver;

    @Autowired
    private Sender sender;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;


    @Test
    public void testReceive() throws Exception {
        //sender.send(C3TOPIC,"Hey Kafka Broker: Naga Here");
        User user = User.newBuilder().setName("User:NSV1").setFavoriteColor("Color:Black").setFavoriteNumber(3).build();
        sender.send(user);

        Thread.sleep(10000);
        receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertThat(receiver.getLatch().getCount()).isEqualTo(0);
    }

}