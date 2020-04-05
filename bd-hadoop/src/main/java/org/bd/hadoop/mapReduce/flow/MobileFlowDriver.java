package org.bd.hadoop.mapReduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bd.hadoop.mapReduce.count.WordCountMapper;
import org.bd.hadoop.mapReduce.count.WordCountReducer;


/**
 * Created by shifeifei on 2020/4/2.
 */
public class MobileFlowDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MobileFlowDriver.class);

        job.setMapperClass(MobileFlowMap.class);
        job.setReducerClass(MobileFlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MobileFlow.class);

        job.setOutputValueClass(MobileFlow.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/shifeifei/Work/Project/bd-technology/input/mobile.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/shifeifei/Work/Project/bd-technology/output"));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
