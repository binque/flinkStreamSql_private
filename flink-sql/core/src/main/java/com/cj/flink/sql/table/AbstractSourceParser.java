package com.cj.flink.sql.table;

import com.cj.flink.sql.util.MathUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractSourceParser extends AbstractTableParser {

    private static final String VIRTUAL_KEY = "virtualFieldKey";
    private static final String WATERMARK_KEY = "waterMarkKey";
    private static final String NOTNULL_KEY = "notNullKey";

    private static Pattern virtualFieldKeyPattern = Pattern.compile("(?i)^(\\S+\\([^\\)]+\\))\\s+AS\\s+(\\w+)$");
    private static Pattern waterMarkKeyPattern = Pattern.compile("(?i)^\\s*WATERMARK\\s+FOR\\s+(\\S+)\\s+AS\\s+withOffset\\(\\s*(\\S+)\\s*,\\s*(\\d+)\\s*\\)$");
    private static Pattern notNullKeyPattern = Pattern.compile("(?i)^(\\w+)\\s+(\\w+)\\s+NOT\\s+NULL?$");

    public AbstractSourceParser() {
        addParserHandler(VIRTUAL_KEY, virtualFieldKeyPattern, this::dealVirtualField);
        addParserHandler(WATERMARK_KEY, waterMarkKeyPattern, this::dealWaterMark);
        addParserHandler(NOTNULL_KEY, notNullKeyPattern, this::dealNotNull);
    }

    //len(ddd) as len_ddd
    protected void dealVirtualField(Matcher matcher, AbstractTableInfo tableInfo){
        AbstractSourceTableInfo sourceTableInfo = (AbstractSourceTableInfo) tableInfo;
        String fieldName = matcher.group(2);             //len_ddd
        String expression = matcher.group(1);            //len(ddd)
        sourceTableInfo.addVirtualField(fieldName, expression);
    }

    // WATERMARK FOR colName AS withOffset( colName , delayTime )
    protected void dealWaterMark(Matcher matcher, AbstractTableInfo tableInfo){
        AbstractSourceTableInfo sourceTableInfo = (AbstractSourceTableInfo) tableInfo;
        String eventTimeField = matcher.group(1);                         //colName
        //FIXME Temporarily resolve the second parameter row_time_field
        Integer offset = MathUtil.getIntegerVal(matcher.group(3));       //delayTime
        sourceTableInfo.setEventTimeField(eventTimeField);
        sourceTableInfo.setMaxOutOrderness(offset);
    }


    //name string NOT NULL
    protected void dealNotNull(Matcher matcher, AbstractTableInfo tableInfo) {
        String fieldName = matcher.group(1);    //name
        String fieldType = matcher.group(2);    //string
        Class fieldClass= dbTypeConvertToJavaType(fieldType);
        AbstractTableInfo.FieldExtraInfo fieldExtraInfo = new AbstractTableInfo.FieldExtraInfo();
        fieldExtraInfo.setNotNull(true);

        tableInfo.addPhysicalMappings(fieldName, fieldName);
        tableInfo.addField(fieldName);
        tableInfo.addFieldClass(fieldClass);
        tableInfo.addFieldType(fieldType);
        tableInfo.addFieldExtraInfo(fieldExtraInfo);
    }
}
