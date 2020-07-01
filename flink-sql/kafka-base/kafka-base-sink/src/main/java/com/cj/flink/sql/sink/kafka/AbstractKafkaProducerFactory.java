package com.cj.flink.sql.sink.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.runtime.types.CRow;

import com.cj.flink.sql.format.FormatType;
import com.cj.flink.sql.format.SerializationMetricWrapper;
import com.cj.flink.sql.sink.kafka.serialization.AvroCRowSerializationSchema;
import com.cj.flink.sql.sink.kafka.serialization.CsvCRowSerializationSchema;
import com.cj.flink.sql.sink.kafka.serialization.JsonCRowSerializationSchema;
import com.cj.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.Properties;

public abstract class AbstractKafkaProducerFactory {

    /**
     *  获取具体的KafkaProducer
     * eg create KafkaProducer010
     * @param kafkaSinkTableInfo
     * @param typeInformation
     * @param properties
     * @param partitioner
     * @return
     */
    public abstract RichSinkFunction<CRow> createKafkaProducer(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation, Properties properties, Optional<FlinkKafkaPartitioner<CRow>> partitioner, String[] partitionKeys);

    protected SerializationMetricWrapper createSerializationMetricWrapper(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation) {
        SerializationSchema<CRow> serializationSchema = createSerializationSchema(kafkaSinkTableInfo, typeInformation);
        return new SerializationMetricWrapper(serializationSchema);
    }

    private SerializationSchema<CRow> createSerializationSchema(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation) {
        SerializationSchema<CRow> serializationSchema = null;
        if (FormatType.JSON.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isNotBlank(kafkaSinkTableInfo.getSchemaString())) {
                serializationSchema = new JsonCRowSerializationSchema(kafkaSinkTableInfo.getSchemaString(), kafkaSinkTableInfo.getUpdateMode());
            } else if (typeInformation != null && typeInformation.getArity() != 0) {
                serializationSchema = new JsonCRowSerializationSchema(typeInformation, kafkaSinkTableInfo.getUpdateMode());
            } else {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }
        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isBlank(kafkaSinkTableInfo.getFieldDelimiter())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.CSV.name() + " must set fieldDelimiter");
            }
            final CsvCRowSerializationSchema.Builder serSchemaBuilder = new CsvCRowSerializationSchema.Builder(typeInformation);
            serSchemaBuilder.setFieldDelimiter(kafkaSinkTableInfo.getFieldDelimiter().toCharArray()[0]);
            serSchemaBuilder.setUpdateMode(kafkaSinkTableInfo.getUpdateMode());

            serializationSchema = serSchemaBuilder.build();
        } else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isBlank(kafkaSinkTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.AVRO.name() + " must set schemaString");
            }
            serializationSchema = new AvroCRowSerializationSchema(kafkaSinkTableInfo.getSchemaString(), kafkaSinkTableInfo.getUpdateMode());
        }

        if (null == serializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSinkTableInfo.getSinkDataType());
        }

        return serializationSchema;
    }
}
