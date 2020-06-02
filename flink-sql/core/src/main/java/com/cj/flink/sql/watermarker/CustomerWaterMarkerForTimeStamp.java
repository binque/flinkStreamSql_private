package com.cj.flink.sql.watermarker;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.TimeZone;

public class CustomerWaterMarkerForTimeStamp extends AbstractCustomerWaterMarker<Row>{

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForTimeStamp.class);

    public CustomerWaterMarkerForTimeStamp(Time maxOutOfOrderness, int pos, String timezone) {
        super(maxOutOfOrderness);
        this.pos = pos;
        this.timezone= TimeZone.getTimeZone(timezone);
    }

    @Override
    public long extractTimestamp(Row row) {
        try {
            Timestamp time = (Timestamp) row.getField(pos);
            long extractTime=time.getTime();
            return getExtractTimestamp(extractTime);
        } catch (RuntimeException e) {
            logger.error("", e);
        }
        return lastTime;
    }
}
