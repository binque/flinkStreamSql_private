package com.cj.flink.sql.watermarker;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.cj.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Strings;

import java.sql.Timestamp;

public class WaterMarkerAssigner {
    public boolean checkNeedAssignWaterMarker(AbstractSourceTableInfo tableInfo){
        if(Strings.isNullOrEmpty(tableInfo.getEventTimeField())){
            return false;
        }

        return true;
    }

    public DataStream assignWaterMarker(DataStream<Row> dataStream, RowTypeInfo typeInfo, AbstractSourceTableInfo sourceTableInfo){
        String eventTimeFieldName = sourceTableInfo.getEventTimeField();

        int maxOutOrderness = sourceTableInfo.getMaxOutOrderness();

        String timeZone=sourceTableInfo.getTimeZone();

        String[] fieldNames = typeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = typeInfo.getFieldTypes();

        if(Strings.isNullOrEmpty(eventTimeFieldName)){
            return dataStream;
        }

        int pos = -1;
        for(int i=0; i<fieldNames.length; i++){
            if(eventTimeFieldName.equals(fieldNames[i])){
                pos = i;
            }
        }

        Preconditions.checkState(pos != -1, "can not find specified eventTime field:" +
                eventTimeFieldName + " in defined fields.");

        TypeInformation fieldType = fieldTypes[pos];

        AbstractCustomerWaterMarker waterMarker = null;
        if(fieldType.getTypeClass().isAssignableFrom(Timestamp.class)){
            waterMarker = new CustomerWaterMarkerForTimeStamp(Time.milliseconds(maxOutOrderness), pos,timeZone);
        }else if(fieldType.getTypeClass().isAssignableFrom(Long.class)){
            waterMarker = new CustomerWaterMarkerForLong(Time.milliseconds(maxOutOrderness), pos,timeZone);
        }else{
            throw new IllegalArgumentException("not support type of " + fieldType + ", current only support(timestamp, long).");
        }

        String fromTag = "Source:" + sourceTableInfo.getName();
        waterMarker.setFromSourceTag(fromTag);
        return dataStream.assignTimestampsAndWatermarks(waterMarker);
    }
}
