package wmy.hadoop.mapreduce.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/6 11:27
 *@Version 1.0
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {
    private FSDataOutputStream hgy_bigdata;
    private FSDataOutputStream other;

    /**
     * 初始化方法
     */
    public void initialize(TaskAttemptContext job) throws IOException {
        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        // 创建输出流
        hgy_bigdata = fileSystem.create(new Path(outdir+"/hgy_bigdata.log"));
        other = fileSystem.create(new Path(outdir+"/other.log"));
    }

    /**
     * 将（K,V）写出，每对调用一次
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if (out.contains("hgy_bigdata")) {
            hgy_bigdata.write(out.getBytes());
        } else {
            other.write(out.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(hgy_bigdata);
        IOUtils.closeStream(other);
    }
}