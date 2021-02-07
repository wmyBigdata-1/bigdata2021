package wmy.hadoop.mapreduce.groupingComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/6 8:30
 *@Version 1.0
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取数据 0000001	Pdt_01	222.8
        String[] fields = value.toString().split("\t");
        orderBean.setOrderId(fields[0]);
        orderBean.setProductId(fields[1]);
        orderBean.setPrice(Double.parseDouble(fields[2]));
        context.write(orderBean, NullWritable.get());
    }
}