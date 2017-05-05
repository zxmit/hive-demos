package com.zxm.hive.hcatalog;

import org.apache.derby.impl.sql.compile.TableName;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zxm on 2017/5/4.
 */
public class DataWriter {


    public static void write(String dbName, String tableName) throws HCatException {
        WriteEntity.Builder builder = new WriteEntity.Builder();
        WriteEntity entity = builder.withDatabase(dbName).withTable(tableName).build();


        Map<String, String> config = new HashMap<String, String>();
        HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);

        WriterContext context = writer.prepareWrite();

        HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);

        List<HCatRecord> records = new ArrayList<HCatRecord>();

        List<Object> list = new ArrayList<Object>();
        list.add("6");
        list.add("周六");
        list.add("31");
        list.add("187");
        records.add(new DefaultHCatRecord(list));

        splitWriter.write(records.iterator());
        writer.commit(context);
        System.exit(0);

    }

    public static void main(String[] args) throws HCatException {
        if(args.length < 2) {
            System.err.println("dbName tableName is needed!");
            System.exit(0);
        }
        String dbName = args[0];
        String tableName = args[1];
        write(dbName, tableName);
    }


}
