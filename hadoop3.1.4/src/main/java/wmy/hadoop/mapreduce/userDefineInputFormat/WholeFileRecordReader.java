package wmy.hadoop.mapreduce.userDefineInputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
 *@description:实现自定义的RecordReader
 * 六个方法：看源码
 * 每个RecordReader处理一个文件，把一个文件读取一个KV值，一次把一个文件全部读取完毕
 * 在netkeyvalue开流就在哪里关，在init开流，在close关，自然的话就要弄一个成员变量
 *@author: 情深@骚明
 *@time: 2021/2/4 8:35
 *@Version 1.0
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
    // 记录读取文件的进度
    private boolean notRead = true;

    // K,V跨方法了
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    // 开流的成员变量
    private FSDataInputStream fsDataInputStream;

    // FileSplit的成员变量
    private FileSplit fs;

    /**
     * 初始化方法，框架在开始的时候调用一次
     * @param inputSplit the split that defines the range of records to read
     * @param taskAttemptContext the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 最终我们得到的切片的范围，我们要使用FileSplit
        // InputSplit ---> 使用多态
        // 转换切片类型到文件切片，通过切片获取路径，通过路径获取文件系统，开流
        fs = (FileSplit) inputSplit;
        Path path = fs.getPath();
        // 需要传一个conf，我们在任务中，所有的信息在taskAttemptContext都有
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        fsDataInputStream = fileSystem.open(path);
    }

    /**
     * 尝试读取一下组（K,V）如果读到了返回true，读完了返回false
     * 第一次返回true，第二次返回false
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead) {
            // 具体读文件的过程
            // 读key
            key.set(fs.getPath().toString());

            // 读value
            // 我们一次性把文件读取完毕，缓冲区要和文件的长度是一样的
            byte[] buf = new byte[(int) fs.getLength()];
            fsDataInputStream.read(buf);
            value.set(buf,0,buf.length);

            notRead = false;
            return true;
        } else {
            // 表示文件以及读取完毕了
            return false;
        }
    }

    /**
     * 获取当前读到的key
     * @return 当前的key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前读到的Value
     * @return 当前的Value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * The current progress of the record reader through its data.
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // 有fsDataInputStream开流就有关流，有一个现成的工具
        IOUtils.closeStream(fsDataInputStream);
    }
}