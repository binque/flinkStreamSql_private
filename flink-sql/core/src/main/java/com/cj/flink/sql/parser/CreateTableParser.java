package com.cj.flink.sql.parser;


import com.cj.flink.sql.util.DtStringUtil;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CREATE TABLE MyTable (
 * 	 userID VARCHAR ,
 * 	 eventType VARCHAR,
 * 	 productID VARCHAR)
 * WITH (
 * 	type = 'kafka11',
 * 	bootstrapServers = '172.16.8.107:9092',
 * 	zookeeperQuorum = '172.16.8.107:2181/kafka',
 * 	offsetReset = 'latest',
 * 	topic ='mqTest03',
 * 	topicIsPattern = 'false'
 * );
 *
 *
 * group(0)  MyTable
 * group（1）
 *       userID VARCHAR ,
 *  * 	 eventType VARCHAR,
 *  * 	 productID VARCHAR
 *
 *  group(2)
 *  	type = 'kafka11',
 *  * 	bootstrapServers = '172.16.8.107:9092',
 *  * 	zookeeperQuorum = '172.16.8.107:2181/kafka',
 *  * 	offsetReset = 'latest',
 *  * 	topic ='mqTest03',
 *  * 	topicIsPattern = 'false'
 */
public class CreateTableParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateTableParser newInstance(){
        return new CreateTableParser();
    }

    @Override
    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()){
            //table的名称
            String tableName = matcher.group(1);
            //字段名称
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);

            //table的连接参数名称
            Map<String, Object> props = parseProp(propsStr);

            SqlParserResult result = new SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            sqlTree.addPreDealTableInfo(tableName, result);

        }
    }

    private Map parseProp(String propsStr){
        String[] strs = propsStr.trim().split("'\\s*,");
        Map<String, Object> propMap = Maps.newHashMap();
        for(int i=0; i<strs.length; i++){
            List<String> ss = DtStringUtil.splitIgnoreQuota(strs[i], '=');
            String key = ss.get(0).trim();
            String value = ss.get(1).trim().replaceAll("'", "").trim();
            propMap.put(key, value);
        }

        return propMap;
    }

    public static class SqlParserResult{

        private String tableName;

        private String fieldsInfoStr;

        private Map<String, Object> propMap;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public Map<String, Object> getPropMap() {
            return propMap;
        }

        public void setPropMap(Map<String, Object> propMap) {
            this.propMap = propMap;
        }
    }
}
