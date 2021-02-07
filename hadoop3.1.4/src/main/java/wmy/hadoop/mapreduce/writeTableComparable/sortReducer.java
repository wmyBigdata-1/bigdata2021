package wmy.hadoop.mapreduce.writeTableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/5 8:36
 *@Version 1.0
 */
public class sortReducer extends Reducer<flowBean, Text, Text, flowBean> {
    @Override
    protected void reduce(flowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}