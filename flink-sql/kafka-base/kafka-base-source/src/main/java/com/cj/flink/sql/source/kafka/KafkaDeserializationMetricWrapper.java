package com.cj.flink.sql.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;

import com.cj.flink.sql.format.DeserializationMetricWrapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cj.flink.sql.metric.MetricConstant.DT_PARTITION_GROUP;
import static com.cj.flink.sql.metric.MetricConstant.DT_TOPIC_GROUP;
import static com.cj.flink.sql.metric.MetricConstant.DT_TOPIC_PARTITION_LAG_GAUGE;

public class KafkaDeserializationMetricWrapper extends DeserializationMetricWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDeserializationMetricWrapper.class);

    private AbstractFetcher<Row, ?> fetcher;

    private AtomicBoolean firstMsg = new AtomicBoolean(true);

    private Calculate calculate;

    public KafkaDeserializationMetricWrapper(TypeInformation<Row> typeInfo, DeserializationSchema<Row> deserializationSchema, Calculate calculate) {
        super(typeInfo, deserializationSchema);
        this.calculate = calculate;
    }

    //在序列化之前
    @Override
    protected void beforeDeserialize() throws IOException {
        super.beforeDeserialize();
        if (firstMsg.compareAndSet(true, false)) {
            try {
                registerPtMetric(fetcher);
            } catch (Exception e) {
                LOG.error("register topic partition metric error.", e);
            }
        }
    }

    protected void registerPtMetric(AbstractFetcher<Row, ?> fetcher) throws Exception {
        Field consumerThreadField = getConsumerThreadField(fetcher);
        consumerThreadField.setAccessible(true);
        KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

        Field hasAssignedPartitionsField = consumerThread.getClass().getDeclaredField("hasAssignedPartitions");

        //是否分配分区
        boolean hasAssignedPartitions = (boolean) hasAssignedPartitionsField.get(consumerThread);

        if (!hasAssignedPartitions) {
            throw new RuntimeException("wait 50 secs, but not assignedPartitions");
        }

        Field consumerField = consumerThread.getClass().getDeclaredField("consumer");
        consumerField.setAccessible(true);

        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerField.get(consumerThread);
        //这个维护了kafka的消费状态
        Field subscriptionStateField = kafkaConsumer.getClass().getDeclaredField("subscriptions");
        subscriptionStateField.setAccessible(true);

        //topic partitions lag
        SubscriptionState subscriptionState = (SubscriptionState) subscriptionStateField.get(kafkaConsumer);
        //获取到
        Set<TopicPartition> assignedPartitions = subscriptionState.assignedPartitions();

        for (TopicPartition topicPartition : assignedPartitions) {
            MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(DT_TOPIC_GROUP, topicPartition.topic())
                    .addGroup(DT_PARTITION_GROUP, topicPartition.partition() + "");
            metricGroup.gauge(DT_TOPIC_PARTITION_LAG_GAUGE, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return calculate.calc(subscriptionState, topicPartition);
                }
            });
        }

    }

    public void setFetcher(AbstractFetcher<Row, ?> fetcher) {
        this.fetcher = fetcher;
    }

    private Field getConsumerThreadField(AbstractFetcher fetcher) throws NoSuchFieldException {
        try {
            return fetcher.getClass().getDeclaredField("consumerThread");
        } catch (Exception e) {
            return fetcher.getClass().getSuperclass().getDeclaredField("consumerThread");
        }
    }
}
