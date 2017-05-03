package com.zxm.hive.loader;

import java.util.Map;

/**
 * Created by zxm on 2017/5/3.
 */
public class HiveOptions {


    private String[] columnNames;

    private Map<String, String> colTypeMap;

    private String databaseName;

    private String tableName;

    private String partitionKey;

    private char fieldDelim;

    private char recordDelim;

    private String compressionCodec;

    private String hivePartitionValue;

    private String warehouseDir;

    private String targetDir;

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public Map<String, String> getColTypeMap() {
        return colTypeMap;
    }

    public void setColTypeMap(Map<String, String> colTypeMap) {
        this.colTypeMap = colTypeMap;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public char getFieldDelim() {
        return fieldDelim;
    }

    public void setFieldDelim(char fieldDelim) {
        this.fieldDelim = fieldDelim;
    }

    public String getCompressionCodec() {
        return compressionCodec;
    }

    public void setCompressionCodec(String compressionCodec) {
        this.compressionCodec = compressionCodec;
    }

    public String getHivePartitionValue() {
        return hivePartitionValue;
    }

    public void setHivePartitionValue(String hivePartitionValue) {
        this.hivePartitionValue = hivePartitionValue;
    }

    public String getWarehouseDir() {
        return warehouseDir;
    }

    public void setWarehouseDir(String warehouseDir) {
        this.warehouseDir = warehouseDir;
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public char getRecordDelim() {
        return recordDelim;
    }

    public void setRecordDelim(char recordDelim) {
        this.recordDelim = recordDelim;
    }
}
