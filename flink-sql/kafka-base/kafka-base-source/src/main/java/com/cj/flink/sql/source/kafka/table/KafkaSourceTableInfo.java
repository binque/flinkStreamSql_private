package com.cj.flink.sql.source.kafka.table;

import com.cj.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaSourceTableInfo extends AbstractSourceTableInfo {
    public static final String BOOTSTRAPSERVERS_KEY = "bootstrapServers";

    public static final String TOPIC_KEY = "topic";

    public static final String TYPE_KEY = "type";

    public static final String GROUPID_KEY = "groupId";

    public static final String OFFSETRESET_KEY = "offsetReset";

    public static final String TOPICISPATTERN_KEY = "topicIsPattern";

    public static final String SCHEMA_STRING_KEY = "schemaInfo";

    public static final String CSV_FIELD_DELIMITER_KEY = "fieldDelimiter";

    public static final String SOURCE_DATA_TYPE_KEY = "sourceDataType";

    private String bootstrapServers;

    private String topic;

    private String groupId;

    private String offsetReset;

    private Boolean topicIsPattern = false;

    private String sourceDataType;

    private String schemaString;

    private String fieldDelimiter;

    //这里存放kafka的参数 以kafka开始+ 参数名
    //eg : kafka.consumer.id
    public Map<String, String> kafkaParams = new HashMap<>();

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

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public void setOffsetReset(String offsetReset) {
        this.offsetReset = offsetReset;
    }

    public Boolean getTopicIsPattern() {
        return topicIsPattern;
    }

    public void setTopicIsPattern(Boolean topicIsPattern) {
        this.topicIsPattern = topicIsPattern;
    }

    public void addKafkaParam(Map<String, String> kafkaParam) {
        kafkaParams.putAll(kafkaParam);
    }

    public String getKafkaParam(String key) {
        return kafkaParams.get(key);
    }

    public Set<String> getKafkaParamKeys() {
        return kafkaParams.keySet();
    }

    public String getSourceDataType() {
        return sourceDataType;
    }

    public void setSourceDataType(String sourceDataType) {
        this.sourceDataType = sourceDataType;
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

    @Override
    public boolean check() {
        Preconditions.checkNotNull(getType(), "kafka of type is required");
        Preconditions.checkNotNull(bootstrapServers, "kafka of bootstrapServers is required");
        Preconditions.checkNotNull(topic, "kafka of topic is required");
        return false;
    }
}
