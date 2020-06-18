package com.cj.flink.sql.source.kafka.table;

import com.cj.flink.sql.format.FormatType;
import com.cj.flink.sql.source.kafka.enums.EKafkaOffset;
import com.cj.flink.sql.table.AbstractSourceParser;
import com.cj.flink.sql.table.AbstractTableInfo;
import com.cj.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.stream.Collectors;

public class KafkaSourceParser extends AbstractSourceParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        KafkaSourceTableInfo kafkaSourceTableInfo = new KafkaSourceTableInfo();
        parseFieldsInfo(fieldsInfo, kafkaSourceTableInfo);

        kafkaSourceTableInfo.setName(tableName);
        kafkaSourceTableInfo.setType(MathUtil.getString(props.get(KafkaSourceTableInfo.TYPE_KEY.toLowerCase())));
        kafkaSourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(KafkaSourceTableInfo.PARALLELISM_KEY.toLowerCase())));
        kafkaSourceTableInfo.setBootstrapServers(MathUtil.getString(props.get(KafkaSourceTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase())));
        kafkaSourceTableInfo.setGroupId(MathUtil.getString(props.get(KafkaSourceTableInfo.GROUPID_KEY.toLowerCase())));
        kafkaSourceTableInfo.setTopic(MathUtil.getString(props.get(KafkaSourceTableInfo.TOPIC_KEY.toLowerCase())));
        kafkaSourceTableInfo.setOffsetReset(MathUtil.getString(props.get(KafkaSourceTableInfo.OFFSETRESET_KEY.toLowerCase())));
        kafkaSourceTableInfo.setTopicIsPattern(MathUtil.getBoolean(props.get(KafkaSourceTableInfo.TOPICISPATTERN_KEY.toLowerCase()), false));
        kafkaSourceTableInfo.setOffsetReset(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.OFFSETRESET_KEY.toLowerCase(), EKafkaOffset.LATEST.name().toLowerCase())));
        kafkaSourceTableInfo.setTopicIsPattern(MathUtil.getBoolean(props.get(KafkaSourceTableInfo.TOPICISPATTERN_KEY.toLowerCase())));
        kafkaSourceTableInfo.setTimeZone(MathUtil.getString(props.get(KafkaSourceTableInfo.TIME_ZONE_KEY.toLowerCase())));

        //这里一般是avro ,csv才会有的设置
        kafkaSourceTableInfo.setSchemaString(MathUtil.getString(props.get(KafkaSourceTableInfo.SCHEMA_STRING_KEY.toLowerCase())));
        kafkaSourceTableInfo.setFieldDelimiter(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.CSV_FIELD_DELIMITER_KEY.toLowerCase(), "|")));
        kafkaSourceTableInfo.setSourceDataType(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.SOURCE_DATA_TYPE_KEY.toLowerCase(), FormatType.DT_NEST.name())));
        Map<String, String> kafkaParams = props.keySet().stream()
                .filter(key -> !key.isEmpty() && key.startsWith("kafka."))
                .collect(Collectors.toMap(
                        key -> key.substring(6), key -> props.get(key).toString())
                );

        kafkaSourceTableInfo.addKafkaParam(kafkaParams);
        kafkaSourceTableInfo.check();

        return kafkaSourceTableInfo;
    }
}
