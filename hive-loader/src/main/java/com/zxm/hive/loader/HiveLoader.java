package com.zxm.hive.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zxm on 2017/5/3.
 */
public class HiveLoader {

    private static String driverName =
            "org.apache.hive.jdbc.HiveDriver";

    public static HiveOptions getHiveOptions() {
        HiveOptions options = new HiveOptions();
        options.setTableName("test_1");
        options.setColumnNames(new String[]{"ID","NAME","AGE","HEIGHT"});
        Map<String, String> colTyepMap = new HashMap<String, String>();
        colTyepMap.put("ID", "STRING");
        colTyepMap.put("NAME", "STRING");
        colTyepMap.put("AGE", "STRING");
        colTyepMap.put("HEIGHT", "STRING");

        options.setColTypeMap(colTyepMap);

        options.setCompressionCodec("snappy");
        options.setFieldDelim('\t');
        options.setRecordDelim('\n');
        options.setWarehouseDir("/tmp/zxm");

        return options;
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection("jdbc:hive2://172.16.18.80:10000/default","root","");
        Statement stmt = conn.createStatement();

        HiveOptions options = getHiveOptions();
        Test test = new Test(options);
        String sql1 = test.getCreateTableStmt();
        System.out.println(sql1);
//        boolean r1 = stmt.execute(sql1);
//        System.out.println(r1);
        String sql2 = test.getLoadDataStmt();
        System.out.println(sql2);

        boolean r2 = stmt.execute(sql2);
//        System.out.println(r2);

        stmt.close();
        conn.close();


    }
}
