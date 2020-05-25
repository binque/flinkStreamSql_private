package com.cj.flink.sql.table;

import com.cj.flink.sql.util.ClassUtil;
import com.cj.flink.sql.util.DtStringUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractTableParser {

    private static final String PRIMARY_KEY = "primaryKey";
    private static final String NEST_JSON_FIELD_KEY = "nestFieldKey";
    private static final String CHAR_TYPE_NO_LENGTH = "CHAR";


    /**
     * CREATE TABLE sideTable(
     *     cf:name varchar as name,
     *     cf:info varchar as info,
     *     PRIMARY KEY(name,age,sss),
     *     PERIOD FOR SYSTEM_TIME
     *  )WITH()
     */
    private static Pattern primaryKeyPattern = Pattern.compile("(?i)PRIMARY\\s+KEY\\s*\\((.*)\\)");
    //                                                                            ff              aa      AS      bb       NOT NULL
    //                                                                      ii(xxx)ff              aa      AS      bb       NOT NULL
    //                                                                      ii(xxx)ff              aa      AS      bb
    private static Pattern nestJsonFieldKeyPattern = Pattern.compile("(?i)((@*\\S+\\.)*\\S+)\\s+(\\w+)\\s+AS\\s+(\\w+)(\\s+NOT\\s+NULL)?$");

    //strLen(name)
    private static Pattern physicalFieldFunPattern = Pattern.compile("\\w+\\((\\w+)\\)$");
    private static Pattern charTypePattern = Pattern.compile("(?i)CHAR\\((\\d*)\\)$");

    private Map<String, Pattern> patternMap = Maps.newHashMap();

    private Map<String, ITableFieldDealHandler> handlerMap = Maps.newHashMap();

    public AbstractTableParser() {
        addParserHandler(PRIMARY_KEY, primaryKeyPattern, this::dealPrimaryKey);
        addParserHandler(NEST_JSON_FIELD_KEY, nestJsonFieldKeyPattern, this::dealNestField);
    }

    protected boolean fieldNameNeedsUpperCase() {
        return true;
    }

    public abstract AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception;


    /**
     * CREATE TABLE sideTable(
     *     cf:name varchar as name,
     *     cf:info varchar as info,
     *     PRIMARY KEY(name,age,sss),
     *     PERIOD FOR SYSTEM_TIME
     *  )
     *  fieldsInfo 就是括号中的字符串
     */
    public void parseFieldsInfo(String fieldsInfo, AbstractTableInfo tableInfo){
        List<String> fieldRows = DtStringUtil.splitIgnoreQuota(fieldsInfo, ',');
        for(String fieldRow : fieldRows) {
            fieldRow = fieldRow.trim();
            if(StringUtils.isBlank(fieldRow)){
                throw new RuntimeException(String.format("table [%s],exists field empty.", tableInfo.getName()));
            }

            //匹配任何空白字符，包括空格、制表符、换页符等。与 [ \f\n\r\t\v] 等效
            // cf:name                  varchar               as               name
            String[] filedInfoArr = fieldRow.split("\\s+");
            if(filedInfoArr.length < 2 ){
                throw new RuntimeException(String.format("table [%s] field [%s] format error.", tableInfo.getName(), fieldRow));
            }

            //这里会把primary key   和  name string as name1 (not null) 的
            boolean isMatcherKey = dealKeyPattern(fieldRow, tableInfo);

            if(isMatcherKey){
                continue;
            }

            //Compatible situation may arise in space in the fieldName
            //这里是会处理   name string   这种信息的字段
            String[] filedNameArr = new String[filedInfoArr.length - 1];
            System.arraycopy(filedInfoArr, 0, filedNameArr, 0, filedInfoArr.length - 1);
            String fieldName = String.join(" ", filedNameArr);
            String fieldType = filedInfoArr[filedInfoArr.length - 1 ].trim();

            Class fieldClass = null;
            AbstractTableInfo.FieldExtraInfo fieldExtraInfo = null;

            Matcher matcher = charTypePattern.matcher(fieldType);
            if (matcher.find()) {
                fieldClass = dbTypeConvertToJavaType(CHAR_TYPE_NO_LENGTH);
                fieldExtraInfo = new AbstractTableInfo.FieldExtraInfo();
                fieldExtraInfo.setLength(Integer.valueOf(matcher.group(1)));
            } else {
                fieldClass = dbTypeConvertToJavaType(fieldType);
            }

            tableInfo.addPhysicalMappings(filedInfoArr[0],filedInfoArr[0]);   //{name, name}
            tableInfo.addField(fieldName);                                    //name string
            tableInfo.addFieldClass(fieldClass);                              //String.class
            tableInfo.addFieldType(fieldType);                                //string
            tableInfo.addFieldExtraInfo(fieldExtraInfo);
        }

        tableInfo.finish();
    }

    public boolean dealKeyPattern(String fieldRow, AbstractTableInfo tableInfo){
        for(Map.Entry<String, Pattern> keyPattern : patternMap.entrySet()){
            Pattern pattern = keyPattern.getValue();
            String key = keyPattern.getKey();
            Matcher matcher = pattern.matcher(fieldRow);
            if(matcher.find()){
                ITableFieldDealHandler handler = handlerMap.get(key);
                if(handler == null){
                    throw new RuntimeException("parse field [" + fieldRow + "] error.");
                }

                handler.dealPrimaryKey(matcher, tableInfo);
                return true;
            }
        }

        return false;
    }

    protected void dealNestField(Matcher matcher, AbstractTableInfo tableInfo) {
        String physicalField = matcher.group(1);
        Preconditions.checkArgument(!physicalFieldFunPattern.matcher(physicalField).find(),
                "No need to add data types when using functions, The correct way is : strLen(name) as nameSize, ");

        String fieldType = matcher.group(3);

        //别名
        String mappingField = matcher.group(4);
        Class fieldClass = dbTypeConvertToJavaType(fieldType);
        boolean notNull = matcher.group(5) != null;
        AbstractTableInfo.FieldExtraInfo fieldExtraInfo = new AbstractTableInfo.FieldExtraInfo();
        fieldExtraInfo.setNotNull(notNull);

        //别名   实际函数或是字段名称
        tableInfo.addPhysicalMappings(mappingField, physicalField);
        tableInfo.addField(mappingField);
        tableInfo.addFieldClass(fieldClass);
        tableInfo.addFieldType(fieldType);
        tableInfo.addFieldExtraInfo(fieldExtraInfo);
    }

    public Class dbTypeConvertToJavaType(String fieldType) {
        return ClassUtil.stringConvertClass(fieldType);
    }

    public void dealPrimaryKey(Matcher matcher, AbstractTableInfo tableInfo){
        String primaryFields = matcher.group(1).trim();
        String[] splitArry = primaryFields.split(",");
        List<String> primaryKes = Lists.newArrayList(splitArry);
        tableInfo.setPrimaryKeys(primaryKes);
    }

    protected void addParserHandler(String parserName, Pattern pattern, ITableFieldDealHandler handler) {
        patternMap.put(parserName, pattern);
        handlerMap.put(parserName, handler);
    }
}
