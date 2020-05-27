package com.cj.flink.sql.table;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class AbstractTableInfo implements Serializable {

    public static final String PARALLELISM_KEY = "parallelism";

    private String name;

    private String type;

    private String[] fields;

    private String[] fieldTypes;

    private Class<?>[] fieldClasses;

    private final List<String> fieldList = Lists.newArrayList();

    /**key:别名, value: realField */
    private Map<String, String> physicalFields = Maps.newHashMap();

    private final List<String> fieldTypeList = Lists.newArrayList();

    private final List<Class> fieldClassList = Lists.newArrayList();

    private final List<FieldExtraInfo> fieldExtraInfoList = Lists.newArrayList();

    private List<String> primaryKeys;

    private Integer parallelism = -1;

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public abstract boolean check();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String[] getFields() {
        return fields;
    }

    public Class<?>[] getFieldClasses() {
        return fieldClasses;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        if(parallelism == null){
            return;
        }

        if(parallelism <= 0){
            throw new RuntimeException("Abnormal parameter settings: parallelism > 0");
        }

        this.parallelism = parallelism;
    }

    public void addField(String fieldName){
        if (fieldList.contains(fieldName)) {
            throw new RuntimeException("redundancy field name " + fieldName + " in table " + getName());
        }

        fieldList.add(fieldName);
    }

    public void addPhysicalMappings(String aliasName, String physicalFieldName){
        physicalFields.put(aliasName, physicalFieldName);
    }

    public void addFieldClass(Class fieldClass){
        fieldClassList.add(fieldClass);
    }

    public void addFieldType(String fieldType){
        fieldTypeList.add(fieldType);
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setFieldTypes(String[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setFieldClasses(Class<?>[] fieldClasses) {
        this.fieldClasses = fieldClasses;
    }

    public void addFieldExtraInfo(FieldExtraInfo extraInfo) {
        fieldExtraInfoList.add(extraInfo);
    }

    public List<String> getFieldList() {
        return fieldList;
    }

    public List<String> getFieldTypeList() {
        return fieldTypeList;
    }

    public List<Class> getFieldClassList() {
        return fieldClassList;
    }

    public Map<String, String> getPhysicalFields() {
        return physicalFields;
    }

    public List<FieldExtraInfo> getFieldExtraInfoList() {
        return fieldExtraInfoList;
    }

    public void setPhysicalFields(Map<String, String> physicalFields) {
        this.physicalFields = physicalFields;
    }

    public void finish(){
        this.fields = fieldList.toArray(new String[fieldList.size()]);
        this.fieldClasses = fieldClassList.toArray(new Class[fieldClassList.size()]);
        this.fieldTypes = fieldTypeList.toArray(new String[fieldTypeList.size()]);
    }

    /**
     * field extra info，used to store `not null` `default 0`...，
     *
     * now, only support not null
     */
    public static class FieldExtraInfo implements Serializable {

        /**
         * default false：allow field is null
         */
        boolean notNull = false;
        /**
         *  field length,eg.char(4)
         */
        int length;

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public boolean getNotNull() {
            return notNull;
        }

        public void setNotNull(boolean notNull) {
            this.notNull = notNull;
        }

        @Override
        public String toString() {
            return "FieldExtraInfo{" +
                    "notNull=" + notNull +
                    ", length=" + length +
                    '}';
        }
    }
}
