package com.zxm.hive.hcatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zxm on 2017/5/4.
 */
public class SchemaWriter {

    /**
     * 生成HCatSchema
     * @param fieldNames    字段名
     * @param fieldTypeMaps 字段名对应的字段类型
     * @return
     * @throws HCatException
     */
    public static HCatSchema getHcatSchema(String[] fieldNames, Map<String, String> fieldTypeMaps) throws HCatException {

        List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(
                fieldNames.length);

        for(String field : fieldNames) {
            HCatFieldSchema.Type type = HCatFieldSchema.Type
                    .valueOf(fieldTypeMaps.get(field).toLowerCase());
            fieldSchemas
                    .add(new HCatFieldSchema(field,type,""));
        }

        return new HCatSchema(fieldSchemas);
    }

    /**
     *  获取表描述
     * @param tableName
     * @param fields
     * @return
     */
    public static StorageDescriptor getTableDescriptor(String tableName, List<FieldSchema> fields) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat(RCFileInputFormat.class.getName());
        sd.setOutputFormat(RCFileOutputFormat.class.getName());
        sd.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tableName);
        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
        sd.getSerdeInfo().getParameters()
                .put(serdeConstants.SERIALIZATION_FORMAT, "1");
        sd.getSerdeInfo().setSerializationLib(
                org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
                        .getName());
        return sd;
    }

    public static void main(String[] args) throws IOException, TException {
        HiveMetaStoreClient client = null;

        HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());

        client = HCatUtil.getHiveClient(hiveConf);

        if (client.tableExists("dbName", "tableName")) {
            client.dropTable("dbName", "tblName");
        }

        HCatSchema hCatSchema = getHcatSchema(new String[]{}, new HashMap<String, String>());

        List<FieldSchema> fields = HCatUtil.getFieldSchemaList(hCatSchema.getFields());

        Table table = new Table();
        table.setDbName("dbName");
        table.setTableName("tblName");


        StorageDescriptor sd = getTableDescriptor(table.getTableName(), fields);
        table.setSd(sd);

//        Map<String, String> tableParams = new HashMap<String, String>();
//        table.setParameters(tableParams);

        try {
            client.createTable(table);
            System.out.println("Create table successfully!");
        } catch (TException e) {
            e.printStackTrace();
            return;
        } finally {
            client.close();
        }

    }
}
