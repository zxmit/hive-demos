package com.zxm.hive.hcatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

/**
 * Created by zxm on 2017/5/4.
 */
public class SchemaReader {

    /**
     * 打印表信息
     * @param sd
     */
    public static void printFieldInfo(StorageDescriptor sd) {
        List<FieldSchema> fields = sd.getCols();
        for(FieldSchema field : fields) {
            System.out.println(field.getName() + "  " + field.getType());
        }
    }

    public static void main(String[] args) throws IOException, TException {
        HiveMetaStoreClient client = null;

        HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());

        client = HCatUtil.getHiveClient(hiveConf);

        Table table = client.getTable("dbName", "tableName");

        StorageDescriptor sd = table.getSd();

        printFieldInfo(sd);

        // 所有表元数据信息可以通过Table、StorageDescriptor两个对象来获取

    }
}
