package org.bd.hadoop.mapReduce.flow;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/2.
 */
public class MobileFlowMap extends Mapper<LongWritable, Text, Text, MobileFlow> {

    private Text k = new Text();
    MobileFlow v = new MobileFlow();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(",");

        String num = data[1];
        long upload = Long.valueOf(data[3]);
        long download = Long.valueOf(data[4]);

        k.set(num);
        v.setUploadFlow(upload);
        v.setDownLoadFlow(download);
        v.setSumFlow(upload + download);

        context.write(k, v);
    }

}
