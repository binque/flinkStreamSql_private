package com.cj.flink.sql.sink.kafka.table;

import com.cj.flink.sql.enums.EUpdateMode;
import com.cj.flink.sql.format.FormatType;
import com.cj.flink.sql.table.AbstractTableInfo;
import com.cj.flink.sql.table.AbstractTableParser;
import com.cj.flink.sql.util.MathUtil;

import java.util.Map;

public class KafkaSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        KafkaSinkTableInfo kafkaSinkTableInfo = new KafkaSinkTableInfo();
        kafkaSinkTableInfo.setName(tableName);
        kafkaSinkTableInfo.setType(MathUtil.getString(props.get(KafkaSinkTableInfo.TYPE_KEY.toLowerCase())));

        parseFieldsInfo(fieldsInfo, kafkaSinkTableInfo);

        if (props.get(KafkaSinkTableInfo.SINK_DATA_TYPE) != null) {
            kafkaSinkTableInfo.setSinkDataType(props.get(KafkaSinkTableInfo.SINK_DATA_TYPE).toString());
        } else {
            kafkaSinkTableInfo.setSinkDataType(FormatType.JSON.name());
        }

        kafkaSinkTableInfo.setSchemaString(MathUtil.getString(props.get(KafkaSinkTableInfo.SCHEMA_STRING_KEY.toLowerCase())));
        kafkaSinkTableInfo.setFieldDelimiter(MathUtil.getString(props.getOrDefault(KafkaSinkTableInfo.CSV_FIELD_DELIMITER_KEY.toLowerCase(), ",")));
        kafkaSinkTableInfo.setBootstrapServers(MathUtil.getString(props.get(KafkaSinkTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase())));
        kafkaSinkTableInfo.setTopic(MathUtil.getString(props.get(KafkaSinkTableInfo.TOPIC_KEY.toLowerCase())));

        kafkaSinkTableInfo.setEnableKeyPartition(MathUtil.getString(props.get(KafkaSinkTableInfo.ENABLE_KEY_PARTITION_KEY.toLowerCase())));
        kafkaSinkTableInfo.setPartitionKeys(MathUtil.getString(props.get(KafkaSinkTableInfo.PARTITION_KEY.toLowerCase())));
        kafkaSinkTableInfo.setUpdateMode(MathUtil.getString(props.getOrDefault(KafkaSinkTableInfo.UPDATE_KEY.toLowerCase(), EUpdateMode.APPEND.name())));

        Integer parallelism = MathUtil.getIntegerVal(props.get(KafkaSinkTableInfo.PARALLELISM_KEY.toLowerCase()));
        kafkaSinkTableInfo.setParallelism(parallelism);

        for (String key : props.keySet()) {
            if (!key.isEmpty() && key.startsWith("kafka.")) {
                kafkaSinkTableInfo.addKafkaParam(key.substring(6), props.get(key).toString());
            }
        }
        kafkaSinkTableInfo.check();

        return kafkaSinkTableInfo;
    }
}
