package wmy.hadoop.mapreduce.groupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 *@description: NullWritable ---> 里面就是一个单例
 *@author: 情深@骚明
 *@time: 2021/2/6 8:31
 *@Version 1.0
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get()); // 就是最高价格
    }
}