package com.zxm.hive.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.io.CodecMap;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by zxm on 2017/5/3.
 */
public class Test {

    public static final Log LOG = LogFactory.getLog(
            Test.class.getName());

    private static String driverName =
            "org.apache.hadoop.hive.jdbc.HiveDriver";

    private static HiveOptions options;

    public Test(HiveOptions options) {
        this.options = options;
    }

    /**
     * @return the CREATE TABLE statement for the table to load into hive.
     */
    public String getCreateTableStmt() throws IOException {

        Map<String, String> colTypeMapping = options.getColTypeMap();
        String [] colNames = options.getColumnNames();
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE IF NOT EXISTS `");


        if(options.getDatabaseName() != null) {
            sb.append(options.getDatabaseName()).append("`.`");
        }
        sb.append(options.getTableName()).append("` ( ");

        boolean first = true;
        String partitionKey = options.getPartitionKey();
        for (String col : colNames) {
            if (col.equals(partitionKey)) {
                throw new IllegalArgumentException("Partition key " + col + " cannot "
                        + "be a column to import.");
            }

            if (!first) {
                sb.append(", ");
            }

            first = false;

            String hiveColType = colTypeMapping.get(col);

            sb.append('`').append(col).append("` ").append(hiveColType);

        }

        sb.append(") ");


        if (partitionKey != null) {
            sb.append("PARTITIONED BY (")
                    .append(partitionKey)
                    .append(" STRING) ");
        }

        sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
        sb.append(getHiveOctalCharCode((int) options.getFieldDelim()));
        sb.append("' LINES TERMINATED BY '");
        sb.append(getHiveOctalCharCode((int) options.getRecordDelim()));
        String codec = options.getCompressionCodec();
        if (codec != null && (codec.equals(CodecMap.LZOP)
                || codec.equals(CodecMap.getCodecClassName(CodecMap.LZOP)))) {
            sb.append("' STORED AS INPUTFORMAT "
                    + "'com.hadoop.mapred.DeprecatedLzoTextInputFormat'");
            sb.append(" OUTPUTFORMAT "
                    + "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");
        } else {
            sb.append("' STORED AS TEXTFILE");
        }

        LOG.debug("Create statement: " + sb.toString());
        return sb.toString();
    }

    /**
     * @return the LOAD DATA statement to import the data in HDFS into hive.
     */
    public String getLoadDataStmt() throws IOException {
        Path finalPath = getFinalPath();

        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA INPATH '");
        sb.append(finalPath.toString() + "'");

        sb.append(" INTO TABLE `");
        if(options.getDatabaseName() != null) {
            sb.append(options.getDatabaseName()).append("`.`");
        }
        sb.append(options.getTableName());
        sb.append('`');

        if (options.getPartitionKey() != null) {
            sb.append(" PARTITION (")
                    .append(options.getPartitionKey())
                    .append("='").append(options.getHivePartitionValue())
                    .append("')");
        }

        LOG.debug("Load statement: " + sb.toString());
        return sb.toString();
    }


    public Path getFinalPath() throws IOException {
        String warehouseDir = options.getWarehouseDir();
        if (null == warehouseDir) {
            warehouseDir = "";
        } else if (!warehouseDir.endsWith(File.separator)) {
            warehouseDir = warehouseDir + File.separator;
        }

        // Final path is determined in the following order:
        // 1. Use target dir if the user specified.
        // 2. Use input table name.
        String tablePath = null;
        String targetDir = options.getTargetDir();
        if (null != targetDir) {
            tablePath = warehouseDir + targetDir;
        } else {
            tablePath = warehouseDir + options.getTableName();
        }
        FileSystem fs = FileSystem.get(new Configuration());
        return new Path(tablePath).makeQualified(fs);
    }



    /**
     * Return a string identifying the character to use as a delimiter
     * in Hive, in octal representation.
     * Hive can specify delimiter characters in the form '\ooo' where
     * ooo is a three-digit octal number between 000 and 177. Values
     * may not be truncated ('\12' is wrong; '\012' is ok) nor may they
     * be zero-prefixed (e.g., '\0177' is wrong).
     *
     * @param charNum the character to use as a delimiter
     * @return a string of the form "\ooo" where ooo is an octal number
     * in [000, 177].
     * @throws IllegalArgumentException if charNum &gt; 0177.
     */
    public static String getHiveOctalCharCode(int charNum) {
        if (charNum > 0177) {
            throw new IllegalArgumentException(
                    "Character " + charNum + " is an out-of-range delimiter");
        }
        return String.format("\\%03o", charNum);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection("jdbc:hive://gongan03:10000/default","","");
    }
}
