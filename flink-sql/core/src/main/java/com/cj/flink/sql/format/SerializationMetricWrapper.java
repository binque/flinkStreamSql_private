package com.cj.flink.sql.format;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.table.runtime.types.CRow;

import com.cj.flink.sql.metric.MetricConstant;

public class SerializationMetricWrapper implements SerializationSchema<CRow> {

    private static final long serialVersionUID = 1L;

    private SerializationSchema<CRow> serializationSchema;

    private transient RuntimeContext runtimeContext;

    //输出个数
    protected transient Counter dtNumRecordsOut;

    //输出频率
    protected transient Meter dtNumRecordsOutRate;

    public SerializationMetricWrapper(SerializationSchema<CRow> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    public void initMetric() {
        dtNumRecordsOut = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        dtNumRecordsOutRate = runtimeContext.getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(dtNumRecordsOut, 20));
    }

    @Override
    public byte[] serialize(CRow element) {
        beforeSerialize();
        byte[] row = serializationSchema.serialize(element);
        afterSerialize();
        return row;
    }

    protected void beforeSerialize() {
    }

    protected void afterSerialize() {
        dtNumRecordsOut.inc();
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public SerializationSchema<CRow> getSerializationSchema() {
        return serializationSchema;
    }

}
