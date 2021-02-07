package wmy.hadoop.mapreduce.writableComarableAndPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wmy.hadoop.mapreduce.writeTableComparable.flowBean;
import wmy.hadoop.mapreduce.writeTableComparable.sortMapper;
import wmy.hadoop.mapreduce.writeTableComparable.sortReducer;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/5 8:38
 *@Version 1.0
 */
public class sortPartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(sortPartitionDriver.class);
        job.setMapperClass(sortMapper.class);
        job.setReducerClass(sortReducer.class);
        job.setMapOutputKeyClass(flowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);

        job.setPartitionerClass(MyPartitionSort.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}