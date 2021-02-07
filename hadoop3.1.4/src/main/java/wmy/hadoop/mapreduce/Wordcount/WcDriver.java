package wmy.hadoop.mapreduce.Wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 *@description:
 *  设置Map、Reducer信息来符合MapReduce计算框架
 *  相当于YARN集群的客户端，用于提交我们整个程序到YARN集群，提交的是封装MapReduce程序相关的job对象
 *@author: 情深@骚明
 *@time: 2021/2/3 17:06
 *@Version 1.0
 */
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设置客户端身份 以具备权限在hdfs上进行操作
        // System.setProperty("HADOOP_USER_NAME","root");

        // 获取一个Job实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 设置我们的类路径（classpath）
        job.setJarByClass(WcDriver.class);

        // 设置Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);

        // 设置Mapper和Reducer的数据输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setNumReduceTasks(5);

        // 设置数据源
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交我们的job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);

    }
}