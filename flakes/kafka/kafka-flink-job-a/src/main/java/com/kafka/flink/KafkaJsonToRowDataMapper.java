package com.kafka.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.RowData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonToRowDataMapper implements MapFunction<String, RowData> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RowData map(String value) throws Exception {
        JsonNode node = objectMapper.readTree(value);

        String id = node.has("id") ? node.get("id").asText() : "";
        String name = node.has("name") ? node.get("name").asText() : "";
        int clicks = node.has("clicks") ? node.get("clicks").asInt() : 0;
        String createdAt = node.has("created_at") ? node.get("created_at").asText() : "";
        // boolean active = node.has("active") ? node.get("active").asBoolean() : false;
        String status = node.has("status") ? node.get("status").asText() : "UNKNOWN";

        GenericRowData rowData = new GenericRowData(6);
        rowData.setField(0, StringData.fromString(id));
        rowData.setField(1, StringData.fromString(name));
        rowData.setField(2, clicks);
        rowData.setField(3, StringData.fromString(createdAt));
        // rowData.setField(4, active);
        rowData.setField(4, StringData.fromString(status));

        return rowData;
    }
}
