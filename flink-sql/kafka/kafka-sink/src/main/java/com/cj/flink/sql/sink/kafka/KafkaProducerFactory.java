package com.cj.flink.sql.sink.kafka;

import com.cj.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.runtime.types.CRow;

import java.util.Optional;
import java.util.Properties;

public class KafkaProducerFactory extends AbstractKafkaProducerFactory{
    @Override
    public RichSinkFunction<CRow> createKafkaProducer(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation, Properties properties, Optional<FlinkKafkaPartitioner<CRow>> partitioner, String[] partitionKeys) {
        return new KafkaProducer(kafkaSinkTableInfo.getTopic(), createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation), properties, partitioner, partitionKeys);
    }

}
