package com.cj.flink.sql.source.kafka;

import com.cj.flink.sql.format.DeserializationMetricWrapper;
import com.cj.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;
import org.apache.kafka.common.requests.IsolationLevel;

import java.io.Serializable;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerFactory extends AbstractKafkaConsumerFactory {
    @Override
    protected FlinkKafkaConsumerBase<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
        KafkaConsumer kafkaSrc = null;
        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, (Calculate & Serializable) (subscriptionState, tp) -> subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED));
            kafkaSrc = new KafkaConsumer(Pattern.compile(kafkaSourceTableInfo.getTopic()), deserMetricWrapper, props);
        } else {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, (Calculate & Serializable) (subscriptionState, tp) -> subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED));
            kafkaSrc = new KafkaConsumer(kafkaSourceTableInfo.getTopic(), deserMetricWrapper, props);
        }
        return kafkaSrc;
    }
}
