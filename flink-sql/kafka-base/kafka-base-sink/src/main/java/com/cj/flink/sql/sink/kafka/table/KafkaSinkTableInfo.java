package com.cj.flink.sql.sink.kafka.table;

import com.cj.flink.sql.enums.EUpdateMode;
import com.cj.flink.sql.format.FormatType;
import com.cj.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaSinkTableInfo extends AbstractTargetTableInfo {
    public static final String BOOTSTRAPSERVERS_KEY = "bootstrapServers";

    public static final String TOPIC_KEY = "topic";

    public static final String TYPE_KEY = "type";

    public static final String ENABLE_KEY_PARTITION_KEY = "enableKeyPartitions";

    public static final String PARTITION_KEY = "partitionKeys";

    public static final String UPDATE_KEY = "updateMode";

    public static final String SCHEMA_STRING_KEY = "schemaInfo";

    public static final String RETRACT_FIELD_KEY = "retract";

    public static final String CSV_FIELD_DELIMITER_KEY = "fieldDelimiter";

    private String bootstrapServers;

    public Map<String, String> kafkaParams = new HashMap<String, String>();

    private String topic;

    private String schemaString;

    private String fieldDelimiter;

    private String enableKeyPartition;

    private String partitionKeys;

    private String updateMode;

    public void addKafkaParam(String key, String value) {
        kafkaParams.put(key, value);
    }

    public String getKafkaParam(String key) {
        return kafkaParams.get(key);
    }

    public Set<String> getKafkaParamKeys() {
        return kafkaParams.keySet();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSchemaString() {
        return schemaString;
    }

    public void setSchemaString(String schemaString) {
        this.schemaString = schemaString;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(String updateMode) {
        this.updateMode = updateMode;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(getType(), "kafka of type is required");
        Preconditions.checkNotNull(bootstrapServers, "kafka of bootstrapServers is required");
        Preconditions.checkNotNull(topic, "kafka of topic is required");

        if (StringUtils.equalsIgnoreCase(getSinkDataType(), FormatType.AVRO.name())) {
            avroParamCheck();
        }

        return false;
    }

    public void avroParamCheck() {
        Preconditions.checkNotNull(schemaString, "avro type schemaInfo is required");
        if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.UPSERT.name())) {
            Schema schema = new Schema.Parser().parse(schemaString);
            schema.getFields()
                    .stream()
                    .filter(field -> StringUtils.equalsIgnoreCase(field.name(), RETRACT_FIELD_KEY))
                    .findFirst()
                    .orElseThrow(() ->
                            new NullPointerException(String.valueOf("arvo upsert mode the retract attribute must be contained in schemaInfo field ")));
        }
    }

    public String getEnableKeyPartition() {
        return enableKeyPartition;
    }

    public void setEnableKeyPartition(String enableKeyPartition) {
        this.enableKeyPartition = enableKeyPartition;
    }

    public String getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(String partitionKeys) {
        this.partitionKeys = partitionKeys;
    }
}
