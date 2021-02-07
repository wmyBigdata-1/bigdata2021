package wmy.hadoop.mapreduce.outputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/6 11:26
 *@Version 1.0
 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecordWriter myRecordWriter = new MyRecordWriter();
        myRecordWriter.initialize(job);
        return myRecordWriter;
    }
}