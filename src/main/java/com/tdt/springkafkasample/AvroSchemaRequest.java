package com.tdt.springkafkasample;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Builder
@Setter
@Getter
public class AvroSchemaRequest {

    private String schema;
    List<JsonNode> records;

}
