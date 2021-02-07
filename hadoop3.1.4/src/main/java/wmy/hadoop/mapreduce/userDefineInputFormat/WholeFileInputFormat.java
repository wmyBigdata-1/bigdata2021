package wmy.hadoop.mapreduce.userDefineInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/*
 *@description: 自定义FileInputFormat
 * Mapper输入的数据类型来源是哪里来的？
 *  上游：InputFormat
 * BytesWritable：存储一对二进制数值
 *
 * 默认的是文件切片规则，128或者1.1倍为一片
 * 怎么样保证文件不被切开，格式判断isSplitable，返回false，首先保证文件不被切开
 *
 * 三个文件切三片，每个切片对呀MapTask，每个RecordReader对应一个文件
 *@author: 情深@骚明
 *@time: 2021/2/4 8:28
 *@Version 1.0
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WholeFileRecordReader();
    }
}