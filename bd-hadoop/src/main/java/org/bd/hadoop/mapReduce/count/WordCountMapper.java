package org.bd.hadoop.mapReduce.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<LongWritable, Text, Text, IntWritable>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }


}
