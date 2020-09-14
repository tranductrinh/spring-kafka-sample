package com.tdt.springkafkasample;

import com.user.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Consumer {

    @KafkaListener(topics = "user")
    public void consume(ConsumerRecord record) {
        System.out.println(record.value() instanceof User);
        log.info(String.format("Consumed message -> %s", record.toString()));
    }
}