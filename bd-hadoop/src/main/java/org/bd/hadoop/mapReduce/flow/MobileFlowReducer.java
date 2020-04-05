package org.bd.hadoop.mapReduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/2.
 */
public class MobileFlowReducer extends Reducer<Text, MobileFlow, Text, MobileFlow> {

    @Override
    protected void reduce(Text key, Iterable<MobileFlow> values, Context context) throws IOException, InterruptedException {
        long up = 0L;
        long down = 0L;
        long sum = 0L;

        for (MobileFlow value : values) {
            up += value.getUploadFlow();
            down += value.getDownLoadFlow();
            sum += (up + down);
        }
        MobileFlow bean = new MobileFlow();
        bean.setUploadFlow(up);
        bean.setDownLoadFlow(down);
        bean.setSumFlow(sum);
        context.write(key, bean);
    }
}
