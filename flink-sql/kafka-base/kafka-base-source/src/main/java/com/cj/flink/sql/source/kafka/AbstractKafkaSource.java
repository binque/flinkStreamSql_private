package com.cj.flink.sql.source.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.cj.flink.sql.source.IStreamSourceGener;
import com.cj.flink.sql.source.kafka.enums.EKafkaOffset;
import com.cj.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.cj.flink.sql.table.AbstractSourceTableInfo;
import com.cj.flink.sql.util.DtStringUtil;
import com.cj.flink.sql.util.PluginUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractKafkaSource implements IStreamSourceGener<Table> {

    private static final String SOURCE_OPERATOR_NAME_TPL = "${topic}_${table}";

    protected Properties getKafkaProperties(KafkaSourceTableInfo kafkaSourceTableInfo) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceTableInfo.getBootstrapServers());

        if (DtStringUtil.isJson(kafkaSourceTableInfo.getOffsetReset())) {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EKafkaOffset.NONE.name().toLowerCase());
        } else {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaSourceTableInfo.getOffsetReset());
        }

        if (StringUtils.isNotBlank(kafkaSourceTableInfo.getGroupId())) {
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaSourceTableInfo.getGroupId());
        }

        for (String key : kafkaSourceTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, kafkaSourceTableInfo.getKafkaParam(key));
        }
        return props;
    }

    protected String generateOperatorName(String tabName, String topicName) {
        return SOURCE_OPERATOR_NAME_TPL.replace("${topic}", topicName).replace("${table}", tabName);
    }


    protected TypeInformation<Row> getRowTypeInformation(KafkaSourceTableInfo kafkaSourceTableInfo) {
        Class<?>[] fieldClasses = kafkaSourceTableInfo.getFieldClasses();
        TypeInformation[] types = IntStream.range(0, fieldClasses.length)
                .mapToObj(i -> TypeInformation.of(fieldClasses[i]))
                .toArray(TypeInformation[]::new);

        return new RowTypeInfo(types, kafkaSourceTableInfo.getFields());
    }

    protected void setStartPosition(String offset, String topicName, FlinkKafkaConsumerBase<Row> kafkaSrc) {
        if (StringUtils.equalsIgnoreCase(offset, EKafkaOffset.EARLIEST.name())) {
            kafkaSrc.setStartFromEarliest();
        } else if (DtStringUtil.isJson(offset)) {
            Map<KafkaTopicPartition, Long> specificStartupOffsets = buildOffsetMap(offset, topicName);
            kafkaSrc.setStartFromSpecificOffsets(specificStartupOffsets);
        } else {
            kafkaSrc.setStartFromLatest();
        }
    }

    /**
     *    kafka offset,eg.. {"0":12312,"1":12321,"2":12312}
     * @param offsetJson
     * @param topicName
     * @return
     */
    protected Map<KafkaTopicPartition, Long> buildOffsetMap(String offsetJson, String topicName) {
        try {
            Properties properties = PluginUtil.jsonStrToObject(offsetJson, Properties.class);
            Map<String, Object> offsetMap = PluginUtil.objectToMap(properties);
            Map<KafkaTopicPartition, Long> specificStartupOffsets = offsetMap
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            (Map.Entry<String, Object> entry) -> new KafkaTopicPartition(topicName, Integer.valueOf(entry.getKey())),
                            (Map.Entry<String, Object> entry) -> Long.valueOf(entry.getValue().toString()))
                    );

            return specificStartupOffsets;
        } catch (Exception e) {
            throw new RuntimeException("not support offsetReset type:" + offsetJson);
        }
    }

}
