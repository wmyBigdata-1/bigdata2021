package wmy.hadoop.mapreduce.writeTableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/5 8:25
 *@Version 1.0
 */
public class sortMapper extends Mapper<LongWritable, Text, flowBean, Text> {
    // 定义输出类型
    private flowBean flow = new flowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 13470253144	180	180	360
        // 获取数据
        String[] fields = value.toString().split("\t");
        phone.set(fields[0]);
        flow.setUpFlow(Long.parseLong(fields[1]));
        flow.setDownFlow(Long.parseLong(fields[2]));
        flow.setSumFlow(Long.parseLong(fields[3]));
        context.write(flow,phone);
    }
}