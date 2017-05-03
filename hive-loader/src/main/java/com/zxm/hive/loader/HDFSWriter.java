package com.zxm.hive.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zxm on 2017/5/3.
 */
public class HDFSWriter {

    public static void dataLoader(List<String> contents) throws IOException {

        String dst = "/tmp/zxm/test_1";

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        Path dstPath = new Path(dst);
        fs.setOwner(dstPath, "root", "hive");

        FSDataOutputStream outputStream = fs.create(dstPath);
        for(String content : contents) {
            outputStream.write(content.getBytes());
        }

        fs.close();

        System.out.println("文件创建成功！");
    }

    public static void main(String[] args) throws IOException {
        List<String> contents = new ArrayList<String>();
        contents.add("1\t张三\t23\t178\n");
        contents.add("2\t李四\t22\t169\n");
        contents.add("3\t王二\t23\t170\n");
        contents.add("4\t赵五\t24\t184\n");
        dataLoader(contents);
    }
}
