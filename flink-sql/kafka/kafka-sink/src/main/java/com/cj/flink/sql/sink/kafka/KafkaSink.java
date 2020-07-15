package com.cj.flink.sql.sink.kafka;

import com.cj.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.cj.flink.sql.table.AbstractTargetTableInfo;

import java.util.Optional;
import java.util.Properties;

public class KafkaSink  extends AbstractKafkaSink {
    @Override
    public Object genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafkaSinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;

        Properties kafkaProperties = getKafkaProperties(kafkaSinkTableInfo);
        this.tableName = kafkaSinkTableInfo.getName();
        this.topic = kafkaSinkTableInfo.getTopic();
        this.partitioner = Optional.of(new CustomerFlinkPartition<>());
        this.partitionKeys = getPartitionKeys(kafkaSinkTableInfo);
        this.fieldNames = kafkaSinkTableInfo.getFields();
        this.fieldTypes = getTypeInformations(kafkaSinkTableInfo);
        this.schema = buildTableSchema(fieldNames, fieldTypes);
        this.parallelism = kafkaSinkTableInfo.getParallelism();
        this.sinkOperatorName = SINK_OPERATOR_NAME_TPL.replace("${topic}", topic).replace("${table}", tableName);
        this.kafkaProducer = new KafkaProducerFactory().createKafkaProducer(kafkaSinkTableInfo, getRowTypeInfo(), kafkaProperties, partitioner, partitionKeys);
        return this;
    }
}
