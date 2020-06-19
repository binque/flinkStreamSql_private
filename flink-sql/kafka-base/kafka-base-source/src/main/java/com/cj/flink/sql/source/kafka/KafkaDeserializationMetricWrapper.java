package com.cj.flink.sql.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.cj.flink.sql.format.DeserializationMetricWrapper;

public class KafkaDeserializationMetricWrapper extends DeserializationMetricWrapper {
    public KafkaDeserializationMetricWrapper(TypeInformation<Row> typeInfo, DeserializationSchema<Row> deserializationSchema) {
        super(typeInfo, deserializationSchema);
    }
}
