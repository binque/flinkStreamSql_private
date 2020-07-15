package com.cj.flink.sql.sink.kafka;

import com.cj.flink.sql.format.SerializationMetricWrapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.runtime.types.CRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class KafkaProducer extends FlinkKafkaProducer<CRow> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

    private static final long serialVersionUID = 1L;

    private SerializationMetricWrapper serializationMetricWrapper;

    public KafkaProducer(String topicId, SerializationSchema<CRow> serializationSchema, Properties producerConfig, Optional<FlinkKafkaPartitioner<CRow>> customPartitioner, String[] parititonKeys) {
        super(topicId, new CustomerKeyedSerializationSchema((SerializationMetricWrapper)serializationSchema, parititonKeys), producerConfig, customPartitioner);
        this.serializationMetricWrapper = (SerializationMetricWrapper) serializationSchema;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        LOG.warn("---open KafkaProducer--");
        RuntimeContext runtimeContext = getRuntimeContext();
        serializationMetricWrapper.setRuntimeContext(runtimeContext);
        serializationMetricWrapper.initMetric();
        super.open(configuration);
    }
}
