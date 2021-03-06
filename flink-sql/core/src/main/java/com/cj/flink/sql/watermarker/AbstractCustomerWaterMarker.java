package com.cj.flink.sql.watermarker;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.cj.flink.sql.metric.EventDelayGauge;
import com.cj.flink.sql.metric.MetricConstant;
import com.cj.flink.sql.util.MathUtil;

import java.util.TimeZone;

public abstract class AbstractCustomerWaterMarker<T> extends BoundedOutOfOrdernessTimestampExtractor<T> implements RichFunction {

    private static final long serialVersionUID = 1L;

    private String fromSourceTag = "NONE";

    private transient RuntimeContext runtimeContext;

    protected transient EventDelayGauge eventDelayGauge;

    protected int pos;

    protected long lastTime = 0;

    protected TimeZone timezone;

    public AbstractCustomerWaterMarker(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public void open(Configuration configuration) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        if (this.runtimeContext == null) {
            throw new IllegalStateException("The runtime context has not been initialized.");
        } else if (this.runtimeContext instanceof IterationRuntimeContext) {
            return (IterationRuntimeContext) this.runtimeContext;
        } else {
            throw new IllegalStateException("This stub is not part of an iteration step function.");
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        this.runtimeContext = t;
        eventDelayGauge = new EventDelayGauge();
        t.getMetricGroup().getAllVariables().put("<source_tag>", fromSourceTag);
        t.getMetricGroup().gauge(MetricConstant.DT_EVENT_DELAY_GAUGE, eventDelayGauge);
    }
    public void setFromSourceTag(String fromSourceTag) {
        this.fromSourceTag = fromSourceTag;
    }

    //在每次发送warkmark时就会触发
    protected long getExtractTimestamp(Long extractTime){

        lastTime = extractTime + timezone.getOffset(extractTime);

        eventDelayGauge.setDelayTime(MathUtil.getIntegerVal((System.currentTimeMillis() - extractTime)/1000));

        return lastTime;
    }

}
