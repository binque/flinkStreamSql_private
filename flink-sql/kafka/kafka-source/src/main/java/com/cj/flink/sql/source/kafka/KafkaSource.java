package com.cj.flink.sql.source.kafka;

import com.cj.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.cj.flink.sql.table.AbstractSourceTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaSource extends AbstractKafkaSource{
    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        KafkaSourceTableInfo kafkaSourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafkaSourceTableInfo.getTopic();

        Properties kafkaProperties = getKafkaProperties(kafkaSourceTableInfo);
        TypeInformation<Row> typeInformation = getRowTypeInformation(kafkaSourceTableInfo);
        FlinkKafkaConsumer<Row> kafkaSrc = (FlinkKafkaConsumer<Row>) new KafkaConsumerFactory().createKafkaTableSource(kafkaSourceTableInfo, typeInformation, kafkaProperties);
        String sourceOperatorName = generateOperatorName(sourceTableInfo.getName(), topicName);
        DataStreamSource kafkaSource = env.addSource(kafkaSrc, sourceOperatorName, typeInformation);
        kafkaSource.setParallelism(kafkaSourceTableInfo.getParallelism());

        setStartPosition(kafkaSourceTableInfo.getOffsetReset(), topicName, kafkaSrc);
        String fields = StringUtils.join(kafkaSourceTableInfo.getFields(), ",");

        return tableEnv.fromDataStream(kafkaSource, fields);
    }
}
