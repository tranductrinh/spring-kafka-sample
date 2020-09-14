package com.tdt.springkafkasample;

import io.confluent.developer.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Producer {

    private final KafkaTemplate<String, User> kafkaTemplate;

    void sendMessage(User user) {
        this.kafkaTemplate.send("avro-topic", user.getName(), user);
        log.info(String.format("Produced user -> %s", user));
    }

}