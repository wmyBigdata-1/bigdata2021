package wmy.hadoop.mapreduce.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/*
 *@description: D:\input\order.txt D:\output7 file:///D:/input/pd.txt
 *@author: 情深@骚明
 *@time: 2021/2/7 13:08
 *@Version 1.0
 */
public class mapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(mapJoinDriver.class);
        job.setMapperClass(mapJoinMapper.class);

        // 没有Reduce
        job.setNumReduceTasks(0);

        // 增加缓存
        job.addCacheFile(URI.create(args[2]));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}