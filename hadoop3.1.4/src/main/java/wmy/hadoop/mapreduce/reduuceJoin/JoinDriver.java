package wmy.hadoop.mapreduce.reduuceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wmy.hadoop.mapreduce.reduuceJoin.bean.OrderBean;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/7 8:45
 *@Version 1.0
 */
public class JoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(JoinDriver.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);
        job.setGroupingComparatorClass(JoinMapperAfterComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);

        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}