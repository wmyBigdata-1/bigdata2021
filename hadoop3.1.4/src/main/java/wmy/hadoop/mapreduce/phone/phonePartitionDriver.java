package wmy.hadoop.mapreduce.phone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wmy.hadoop.mapreduce.flow.flowBean;
import wmy.hadoop.mapreduce.flow.flowMapper;
import wmy.hadoop.mapreduce.flow.flowReducer;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/3 20:51
 *@Version 1.0
 */
public class phonePartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job实例
        Job job = Job.getInstance(new Configuration());

        // 设置类路径
        job.setJarByClass(phonePartitionDriver.class);

        // 设置Mapper和Reducer
        job.setMapperClass(flowMapper.class);
        job.setReducerClass(flowReducer.class);

        // 设置Mapper和Reducer的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);

        // 设置分区，分区是根据Reduce Task的个数来决定的
        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);

        // 设置输入数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}