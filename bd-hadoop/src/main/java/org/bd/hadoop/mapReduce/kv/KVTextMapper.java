package org.bd.hadoop.mapReduce.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/5.
 * <p>
 * 输入数据：css kkk lll  输出数据：<css,1>
 */


public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {

    private IntWritable v = new IntWritable(1);

    public void map(Text key, Text value,
                    Context context) throws IOException, InterruptedException {
        context.write(key,v);
    }
}
