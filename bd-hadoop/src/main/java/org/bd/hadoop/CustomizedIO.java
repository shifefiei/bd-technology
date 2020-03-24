package org.bd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by shifeifei on 2020/3/14.
 */
public class CustomizedIO {


    public static void main(String[] args) throws Exception {
        //upload();
        download();
    }

    //I/O流上传文件
    public static void upload() throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");

        //利用JDK的输入流，从本地磁盘读取文件
        FileInputStream input = new FileInputStream(new File("./test/upload.txt"));
        //写入到 hdfs 指定目录
        FSDataOutputStream fos = fs.create(new Path("/test/upload.txt"));

        IOUtils.copyBytes(input, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(input);

        fs.close();
    }

    //下载文件
    public static void download() throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");

        FSDataInputStream fis = fs.open(new Path("/test/aaa444.txt"));

        FileOutputStream fos = new FileOutputStream(new File("./test/aaa444.txt"));
        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();
        System.out.println("completed...");
    }


}
