package org.bd.hadoop.mapReduce.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/5.
 * 对每行的单词统计
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word);
        }
        context.write(k, v);
    }
}
