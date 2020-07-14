package com.cj.flink.sql.source.kafka;

import com.cj.flink.sql.format.DeserializationMetricWrapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumer extends FlinkKafkaConsumer<Row> {
    private DeserializationMetricWrapper deserializationMetricWrapper;

    public KafkaConsumer(String topic, DeserializationMetricWrapper deserializationMetricWrapper, Properties props) {
        super(Arrays.asList(StringUtils.split(topic, ",")), deserializationMetricWrapper, props);
        this.deserializationMetricWrapper = deserializationMetricWrapper;
    }

    public KafkaConsumer(Pattern subscriptionPattern, DeserializationMetricWrapper deserializationMetricWrapper, Properties props) {
        super(subscriptionPattern, deserializationMetricWrapper, props);
        this.deserializationMetricWrapper = deserializationMetricWrapper;
    }

    @Override
    public void run(SourceFunction.SourceContext<Row> sourceContext) throws Exception {
        deserializationMetricWrapper.setRuntimeContext(getRuntimeContext());
        deserializationMetricWrapper.initMetric();
        super.run(sourceContext);
    }

    @Override
    protected AbstractFetcher<Row, ?> createFetcher(SourceFunction.SourceContext<Row> sourceContext,
                                                    Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
                                                    SerializedValue<AssignerWithPeriodicWatermarks<Row>> watermarksPeriodic,
                                                    SerializedValue<AssignerWithPunctuatedWatermarks<Row>> watermarksPunctuated,
                                                    StreamingRuntimeContext runtimeContext,
                                                    OffsetCommitMode offsetCommitMode,
                                                    MetricGroup consumerMetricGroup,
                                                    boolean useMetrics) throws Exception {

        AbstractFetcher<Row, ?> fetcher = super.createFetcher(sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarksPeriodic,
                watermarksPunctuated,
                runtimeContext,
                offsetCommitMode,
                consumerMetricGroup,
                useMetrics);

        ((KafkaDeserializationMetricWrapper) deserializationMetricWrapper).setFetcher(fetcher);
        return fetcher;
    }
}
