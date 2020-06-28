package com.cj.flink.sql.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.DTJsonRowDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;

import com.cj.flink.sql.format.DeserializationMetricWrapper;
import com.cj.flink.sql.format.FormatType;
import com.cj.flink.sql.format.dtnest.DtNestRowDeserializationSchema;
import com.cj.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public abstract class AbstractKafkaConsumerFactory {

    protected abstract FlinkKafkaConsumerBase<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo,
                                                                          TypeInformation<Row> typeInformation,
                                                                          Properties props);

    protected DeserializationMetricWrapper createDeserializationMetricWrapper(KafkaSourceTableInfo kafkaSourceTableInfo,
                                                                              TypeInformation<Row> typeInformation,
                                                                              Calculate calculate) {
        return new KafkaDeserializationMetricWrapper(typeInformation,
                createDeserializationSchema(kafkaSourceTableInfo, typeInformation),
                calculate);
    }

    private DeserializationSchema<Row> createDeserializationSchema(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation) {
        DeserializationSchema<Row> deserializationSchema = null;
        if (FormatType.DT_NEST.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            deserializationSchema = new DtNestRowDeserializationSchema(typeInformation, kafkaSourceTableInfo.getPhysicalFields(), kafkaSourceTableInfo.getFieldExtraInfoList());

        }else if (FormatType.JSON.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            if (StringUtils.isNotBlank(kafkaSourceTableInfo.getSchemaString())) {
                //通过json schema 生成TypeInformation
                deserializationSchema = new DTJsonRowDeserializationSchema(kafkaSourceTableInfo.getSchemaString());
            } else if (typeInformation != null && typeInformation.getArity() != 0) {
                deserializationSchema = new DTJsonRowDeserializationSchema(typeInformation);
            } else {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }
        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            if (StringUtils.isBlank(kafkaSourceTableInfo.getFieldDelimiter())) {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.CSV.name() + " must set fieldDelimiter");
            }

            final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInformation);
            deserSchemaBuilder.setFieldDelimiter(kafkaSourceTableInfo.getFieldDelimiter().toCharArray()[0]);
            deserializationSchema = deserSchemaBuilder.build();
        }else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            if (StringUtils.isBlank(kafkaSourceTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.AVRO.name() + " must set schemaString");
            }

            deserializationSchema = new AvroRowDeserializationSchema(kafkaSourceTableInfo.getSchemaString());
        }

        if (null == deserializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSourceTableInfo.getSourceDataType());
        }

        return deserializationSchema;
    }
}
