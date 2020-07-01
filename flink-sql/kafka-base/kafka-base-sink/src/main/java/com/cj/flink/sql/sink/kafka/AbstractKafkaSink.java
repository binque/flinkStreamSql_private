package com.cj.flink.sql.sink.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.cj.flink.sql.sink.IStreamSinkGener;
import com.cj.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

public abstract class AbstractKafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener {

    public static final String SINK_OPERATOR_NAME_TPL = "${topic}_${table}";

    protected String[] fieldNames;
    protected TypeInformation<?>[] fieldTypes;

    protected String[] partitionKeys;
    protected String sinkOperatorName;
    protected Properties properties;
    protected int parallelism;
    protected String topic;
    protected String tableName;

    protected TableSchema schema;
    protected SinkFunction<CRow> kafkaProducer;
    protected Optional<FlinkKafkaPartitioner<CRow>> partitioner;

    protected Properties getKafkaProperties(KafkaSinkTableInfo KafkaSinkTableInfo) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSinkTableInfo.getBootstrapServers());

        for (String key : KafkaSinkTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, KafkaSinkTableInfo.getKafkaParam(key));
        }
        return props;
    }

    protected TypeInformation[] getTypeInformations(KafkaSinkTableInfo kafka11SinkTableInfo) {
        Class<?>[] fieldClasses = kafka11SinkTableInfo.getFieldClasses();
        TypeInformation[] types = IntStream.range(0, fieldClasses.length)
                .mapToObj(i -> TypeInformation.of(fieldClasses[i]))
                .toArray(TypeInformation[]::new);
        return types;
    }

    protected TableSchema buildTableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length, "fieldNames length must equals fieldTypes length !");

        TableSchema.Builder builder = TableSchema.builder();
        IntStream.range(0, fieldTypes.length)
                .forEach(i -> builder.field(fieldNames[i], fieldTypes[i]));

        return builder.build();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        DataStream<CRow> mapDataStream = dataStream
                .map((Tuple2<Boolean, Row> record) -> new CRow(record.f1, record.f0))
                .returns(getRowTypeInfo())
                .setParallelism(parallelism);

        mapDataStream.addSink(kafkaProducer).name(sinkOperatorName);
    }

    public CRowTypeInfo getRowTypeInfo() {
        return new CRowTypeInfo(new RowTypeInfo(fieldTypes, fieldNames));
    }

    protected String[] getPartitionKeys(KafkaSinkTableInfo kafkaSinkTableInfo) {
        if (StringUtils.isNotBlank(kafkaSinkTableInfo.getPartitionKeys())) {
            return StringUtils.split(kafkaSinkTableInfo.getPartitionKeys(), ',');
        }
        return null;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
