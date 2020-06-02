package com.cj.flink.sql.watermarker;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import com.cj.flink.sql.util.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

public class CustomerWaterMarkerForLong extends AbstractCustomerWaterMarker<Row>{
    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForLong.class);

    public CustomerWaterMarkerForLong(Time maxOutOfOrderness, int pos, String timezone) {
        super(maxOutOfOrderness);
        this.pos = pos;
        this.timezone= TimeZone.getTimeZone(timezone);
    }

    @Override
    public long extractTimestamp(Row row) {

        try{
            Long extractTime = MathUtil.getLongVal(row.getField(pos));
            return getExtractTimestamp(extractTime);
        }catch (Exception e){
            logger.error("", e);
        }
        return lastTime;
    }
}
