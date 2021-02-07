package wmy.hadoop.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/3 20:50
 *@Version 1.0
 */
public class flowMapper extends Mapper<LongWritable, Text, Text, flowBean> {
    // 定义输出的类型
    private Text phone = new Text();
    private flowBean flow = new flowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取数据
        String[] fields = value.toString().split("\t");
        // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        phone.set(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        long sumFlow = Long.parseLong(fields[fields.length - 1]);
        flow.set(upFlow,downFlow);
        context.write(phone,flow);

    }
}