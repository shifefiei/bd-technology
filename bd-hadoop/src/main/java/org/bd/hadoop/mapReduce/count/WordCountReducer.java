package org.bd.hadoop.mapReduce.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/3/29.
 * <p>
 * Text : 表输出数据的key
 * IntWritable: 表示输入数据的value
 * Text: 表示输出出数据的key
 * IntWritable : 表示输出数据的value
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }

}
