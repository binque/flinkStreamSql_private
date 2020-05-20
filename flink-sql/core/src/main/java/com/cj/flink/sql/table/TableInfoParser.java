package com.cj.flink.sql.table;

import com.cj.flink.sql.enums.ETableType;
import com.cj.flink.sql.parser.CreateTableParser;
import com.cj.flink.sql.util.MathUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableInfoParser {

    private final static String TYPE_KEY = "type";

    private final static String SIDE_TABLE_SIGN = "(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$";

    private final static Pattern SIDE_PATTERN = Pattern.compile(SIDE_TABLE_SIGN);

    private Map<String, AbsTableParser> sourceTableInfoMap = Maps.newConcurrentMap();

    private  Map<String, AbsTableParser> targetTableInfoMap = Maps.newConcurrentMap();

    private  Map<String, AbsTableParser> sideTableInfoMap = Maps.newConcurrentMap();

    //Parsing loaded plugin
    public TableInfo parseWithTableType(int tableType, CreateTableParser.SqlParserResult parserResult,
                                        String localPluginRoot) throws Exception {
        AbsTableParser absTableParser = null;

        //with 后面的配置信息
        Map<String, Object> props = parserResult.getPropMap();
        String type = MathUtil.getString(props.get(TYPE_KEY));

        if(Strings.isNullOrEmpty(type)){
            throw new RuntimeException("create table statement requires property of type");
        }

        if(tableType == ETableType.SOURCE.getType()){
            boolean isSideTable = checkIsSideTable(parserResult.getFieldsInfoStr());

            //如果不是维表
            if(!isSideTable){
                absTableParser = sourceTableInfoMap.get(type);
                if(absTableParser == null){
//                    absTableParser = StreamSourceFactory.getSqlParser(type, localPluginRoot);
//                    sourceTableInfoMap.put(type, absTableParser);
                }
            }
        }

        return null;
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
