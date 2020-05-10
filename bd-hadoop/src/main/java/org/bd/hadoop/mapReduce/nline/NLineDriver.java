package org.bd.hadoop.mapReduce.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/6.
 */
public class NLineDriver {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置每个切片中划分 三条 记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        //使用NLineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        job.setJarByClass(NLineDriver.class);

        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/shifeifei/Work/Project/bd-technology/input/nline.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/shifeifei/Work/Project/bd-technology/output2"));


    }
}
