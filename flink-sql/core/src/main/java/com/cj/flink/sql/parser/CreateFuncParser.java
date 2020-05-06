package com.cj.flink.sql.parser;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * create table function UDTFOneColumnToMultiColumn with cn.todd.flink180.udflib.UDTFOneColumnToMultiColumn;
 */
public class CreateFuncParser implements IParser {

    private static final String funcPatternStr = "(?i)\\s*create\\s+(scala|table|aggregate)\\s+function\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern funcPattern = Pattern.compile(funcPatternStr);

    @Override
    public boolean verify(String sql) {
         return funcPattern.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = funcPattern.matcher(sql);
        if (matcher.find()){
            //函数类型 scala|table|aggregate
            String type = matcher.group(1);
            //函数名称
            String funcName = matcher.group(2);
            //函数的className
            String className = matcher.group(3);
            SqlParserResult result = new SqlParserResult();
            result.setType(type);
            result.setClassName(className);
            result.setName(funcName);

            sqlTree.addFunc(result);
        }
    }

    public static CreateFuncParser newInstance(){
        return new CreateFuncParser();
    }

    public static class SqlParserResult{

        private String name;

        private String className;

        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
