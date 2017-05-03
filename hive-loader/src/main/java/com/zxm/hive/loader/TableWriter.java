package com.zxm.hive.loader;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.hive.TableDefWriter;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by zxm on 2017/5/3.
 */
public class TableWriter {

    public static final Log LOG = LogFactory.getLog(
            TableDefWriter.class.getName());

    private SqoopOptions options;
    private com.cloudera.sqoop.manager.ConnManager connManager;
    private Configuration configuration;
    private String inputTableName;
    private String outputTableName;
    private boolean commentsEnabled;
    private Map<String, Integer> externalColTypes;

    /**
     * Get the column names to import.
     */
    private String [] getColumnNames() {
        String [] colNames = options.getColumns();
        if (null != colNames) {
            return colNames; // user-specified column names.
        } else if (null != externalColTypes) {
            // Test-injection column mapping. Extract the col names from this.
            ArrayList<String> keyList = new ArrayList<String>();
            for (String key : externalColTypes.keySet()) {
                keyList.add(key);
            }

            return keyList.toArray(new String[keyList.size()]);
        } else if (null != inputTableName) {
            return connManager.getColumnNames(inputTableName);
        } else {
            return connManager.getColumnNamesForQuery(options.getSqlQuery());
        }
    }
}
