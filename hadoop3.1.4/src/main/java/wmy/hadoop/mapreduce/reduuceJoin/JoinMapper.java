package wmy.hadoop.mapreduce.reduuceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import wmy.hadoop.mapreduce.reduuceJoin.bean.OrderBean;

import java.io.IOException;

/*
 *@description: 简单的做了数据封装
 * order:
 *   1001	01	1
 *   1002	02	2
 *   1003	03	3
 *   1004	01	4
 *   1005	02	5
 *   1006	03	6
 *
 * pd：
 *   01	小米
 *   02	华为
 *   03	格力
 *@author: 情深@骚明
 *@time: 2021/2/7 8:22
 *@Version 1.0
 */
public class JoinMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();
    private String fileName;

    /**
     * Called once at the beginning of the task.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit file = (FileSplit) context.getInputSplit();
        fileName = file.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1001	01	1
        String[] fields = value.toString().split("\t");
        if (fileName.equals("order.txt")) {
            orderBean.setId(fields[0]);
            orderBean.setPid(fields[1]);
            orderBean.setAmount(Integer.parseInt(fields[2]));
            // 反复使用，一定要设置为空
            orderBean.setPname("");
        } else {
            // 01	小米
            orderBean.setPid(fields[0]);
            orderBean.setPname(fields[1]);
            orderBean.setId("");
            orderBean.setAmount(0);
        }
        context.write(orderBean,NullWritable.get());
    }
}