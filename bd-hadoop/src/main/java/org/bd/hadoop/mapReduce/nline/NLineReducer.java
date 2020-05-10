package org.bd.hadoop.mapReduce.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/6.
 */
public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
            v.set(count);
        }
        k.set(key);
        context.write(k, v);
    }

}
