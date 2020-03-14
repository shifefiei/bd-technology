package org.bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;

/**
 * Created by shifeifei on 2020/3/14.
 */
public class HDFSClient {


    public static void main(String[] args) throws Exception {
        //connectionDemo();
        //upload();
        download();

    }

    public static void connectionDemo() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://centos-01:9000"); //设置 hdfs 的 nameNode地址

        //防止centos-01的用户和当前系统的用户不一致，有权限验证
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");
        fs.mkdirs(new Path("/test"));
        fs.close();

        System.out.println(fs);
    }

    //从 hdfs 中下载文件
    public static void download() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");

        //fs.copyToLocalFile(new Path("/test/aaa3.txt"),new Path("./test/bbb.txt"));

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/test/aa2.txt"), new Path("./test/bb.txt"), true);

        fs.close();
        System.out.println("completed....");
    }

    //上传文件到 hdfs 中
    public static void upload() throws Exception {
        Configuration conf = new Configuration();
        //上传时参数优先级：客户端代码中设置的值 > ClassPath下的用户自定义配置文件 > 然后是服务器的默认配置
        conf.set("dfs.replication", "2");

        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");
        fs.copyFromLocalFile(new Path("/Users/shifeifei/Work/Project/bd-technology/test/a.txt"), new Path("/test/aaa3.txt"));
        fs.close();

        System.out.println("completed...");
    }


}
