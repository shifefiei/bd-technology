package org.bd.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.Arrays;

/**
 * Created by shifeifei on 2020/3/14.
 * <p>
 * 属性 hdfs 的基本api
 */
public class BaseApiDemo {
    public static void main(String[] args) throws Exception {
        //connectionDemo();
        //upload();
        //download();
        //delete();
        //rename();
        //listFileDetail();
        check();
    }

    //判断是文件还是文件夹
    public static void check() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), new Configuration(), "root");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isDirectory()) {
                System.out.println("dir : " + status.getPath().getName());
            }
            if (status.isFile()) {
                System.out.println(status.getPath().getName());
            }
        }
        fs.close();
    }

    //查看文件详情信息：文件名称、权限、长度、块信息
    public static void listFileDetail() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), new Configuration(), "root");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);//遍历递归目录
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getReplication());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println(Arrays.toString(blockLocation.getNames()));
                System.out.println(Arrays.toString(blockLocation.getHosts()));
                System.out.println(Arrays.toString(blockLocation.getTopologyPaths()));
            }
            System.out.println("===============================");
        }

        fs.close();
    }

    public static void connectionDemo() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://centos-01:9000"); //设置 hdfs 的 nameNode地址

        //防止centos-01的用户和当前系统的用户不一致，有权限验证
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");
        fs.mkdirs(new Path("/delete"));
        fs.close();

        System.out.println(fs);
    }

    //重新命名
    public static void rename() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), new Configuration(), "root");
        fs.rename(new Path("/test/aaa3.txt"), new Path("/test/aaa444.txt"));
        fs.close();

        System.out.println("completed...");
    }


    //删除文件夹
    public static void delete() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-01:9000"), conf, "root");

        fs.delete(new Path("/delete"), true);
        System.out.println("completed...");

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
