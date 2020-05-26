package com.cj.flink.sql.table;

import com.cj.flink.sql.enums.ETableType;
import com.cj.flink.sql.parser.CreateTableParser;
import com.cj.flink.sql.side.AbstractSideTableInfo;
import com.cj.flink.sql.side.StreamSideFactory;
import com.cj.flink.sql.sink.StreamSinkFactory;
import com.cj.flink.sql.source.StreamSourceFactory;
import com.cj.flink.sql.util.MathUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbstractTableInfoParser {

    private final static String TYPE_KEY = "type";

    private final static String SIDE_TABLE_SIGN = "(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$";

    private final static Pattern SIDE_PATTERN = Pattern.compile(SIDE_TABLE_SIGN);

    private Map<String, AbstractTableParser> sourceTableInfoMap = Maps.newConcurrentMap();

    private  Map<String, AbstractTableParser> targetTableInfoMap = Maps.newConcurrentMap();

    private  Map<String, AbstractTableParser> sideTableInfoMap = Maps.newConcurrentMap();

    //Parsing loaded plugin
    public AbstractTableInfo parseWithTableType(int tableType, CreateTableParser.SqlParserResult parserResult,
                                                String localPluginRoot) throws Exception {
        AbstractTableParser absTableParser = null;

        //with 后面的配置信息
        Map<String, Object> props = parserResult.getPropMap();

        /**
         * WITH(
         *     type ='db2',
         *     url ='jdbcUrl',
         *     userName ='userName',
         *     password ='pwd',
         *     tableName ='tableName',
         *     parallelism ='parllNum'
         *  )
         */
        String type = MathUtil.getString(props.get(TYPE_KEY));   //db2

        if(Strings.isNullOrEmpty(type)){
            throw new RuntimeException("create table statement requires property of type");
        }

        if(tableType == ETableType.SOURCE.getType()){
            boolean isSideTable = checkIsSideTable(parserResult.getFieldsInfoStr());

            //如果不是维表
            if(!isSideTable){
                absTableParser = sourceTableInfoMap.get(type);
                if(absTableParser == null){
                    //这里面涉及到插件加载并且返回对应解析Parser
                    absTableParser = StreamSourceFactory.getSqlParser(type, localPluginRoot);

                    //{db2:sourceParser}
                    sourceTableInfoMap.put(type, absTableParser);
                }
            }else {
                absTableParser = sideTableInfoMap.get(type);
                if(absTableParser == null){
                    String cacheType = MathUtil.getString(props.get(AbstractSideTableInfo.CACHE_KEY));
                    absTableParser = StreamSideFactory.getSqlParser(type, localPluginRoot, cacheType);
                    sideTableInfoMap.put(type + cacheType, absTableParser);
                }
            }
        }else if (tableType == ETableType.SINK.getType()){
            absTableParser = targetTableInfoMap.get(type);
            if(absTableParser == null){
                //
                absTableParser = StreamSinkFactory.getSqlParser(type, localPluginRoot);
                targetTableInfoMap.put(type, absTableParser);
            }
        }

        if(absTableParser == null){
            throw new RuntimeException(String.format("not support %s type of table", type));
        }

        Map<String, Object> prop = Maps.newHashMap();

        //Shield case
        parserResult.getPropMap().forEach((key,val) -> prop.put(key.toLowerCase(), val));

        return absTableParser.getTableInfo(parserResult.getTableName(), parserResult.getFieldsInfoStr(), prop);
    }

    /**
     * judge dim table of PERIOD FOR SYSTEM_TIME
     * @param tableField
     * @return
     */
    private static boolean checkIsSideTable(String tableField){
        String[] fieldInfos = StringUtils.split(tableField, ",");
        for(String field : fieldInfos){
            Matcher matcher = SIDE_PATTERN.matcher(field.trim());
            if(matcher.find()){
                return true;
            }
        }

        return false;
    }
}
