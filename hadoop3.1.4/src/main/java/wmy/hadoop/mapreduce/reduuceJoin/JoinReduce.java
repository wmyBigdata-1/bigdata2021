package wmy.hadoop.mapreduce.reduuceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import wmy.hadoop.mapreduce.reduuceJoin.bean.OrderBean;

import java.io.IOException;
import java.util.Iterator;

/*
 *@description:
 * 数据：
 *    01 小米
 *    1001 01 1
 *    1004 01 4
 *@author: 情深@骚明
 *@time: 2021/2/7 8:40
 *@Version 1.0
 */
public class JoinReduce extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        // 数据指针下移，获取第一个OrderBean
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}