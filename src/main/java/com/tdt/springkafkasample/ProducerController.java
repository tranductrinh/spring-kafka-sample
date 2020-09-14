package com.tdt.springkafkasample;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.user.User;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/topics")
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping(value = "/user1")
    public void produce() {
        kafkaTemplate.send("user", new User(UUID.randomUUID().toString(), "Trinh"));
    }

    @PostMapping(value = "/{topic}")
    public void produce(@PathVariable("topic") String topic,
                        @RequestBody AvroSchemaRequest request) {
        Schema schema = new Schema.Parser().parse(request.getSchema());
        List<ProducerRecord<String, Object>> producerRecords = new ArrayList<>();
        request.getRecords().forEach(record -> {
            producerRecords.add(new ProducerRecord<>(topic, toAvro(record, schema)));
        });

        producerRecords.forEach(producerRecord -> kafkaTemplate.send(producerRecord));

    }

    public Object toAvro(JsonNode value, Schema schema) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new JsonMapper().writeValue(out, value);
            DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
            Object object = reader.read(null, new DecoderFactory().jsonDecoder(schema, new ByteArrayInputStream(out.toByteArray())));
            out.close();
            return object;
        } catch (IOException | RuntimeException e) {
            throw new ConversionException("Failed to convert JSON to Avro: " + e.getMessage(), e);
        }
    }
}