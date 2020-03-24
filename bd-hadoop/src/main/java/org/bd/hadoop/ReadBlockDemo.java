package org.bd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by shifeifei on 2020/3/14.
 * 分块读取文件：block0,block1
 */
public class ReadBlockDemo {

    public static void main(String[] args) throws Exception {
        readBlock2();
        /**
         * 合并 block0,block1
         * cat hadoop-2.7.7.tar.gz >> a.file
         * cat hadoop-2.7.7.tar.gz.part2 >> a.file
         * tar -xvf a.file
         */
    }

    //读取 block0 并写入到本地
    public static void read() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");

        FSDataInputStream fis = fs.open(new Path("/data/input/hadoop-2.7.7.tar.gz"));
        FileOutputStream fos = new FileOutputStream(new File("./test/hadoop-2.7.7.tar.gz"));

        //读取 128M 的 block0
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();
        System.out.println("completed....");
    }

    //读取 block1
    public static void readBlock2() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");

        FSDataInputStream fis = fs.open(new Path("/data/input/hadoop-2.7.7.tar.gz"));
        //定位要读去的文件的起始位置
        fis.seek(1024 * 1024 * 128);

        FileOutputStream fos = new FileOutputStream(new File("./test/hadoop-2.7.7.tar.gz.part2"));
        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();
        System.out.println("completed....");
    }


}
