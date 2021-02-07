package wmy.hadoop.mapreduce.Wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 *@description: 记住Hadoop的类型
 *  LongWritable：这个是文本文件中前面的行号
 *  Text：以这行数据为Value
 *  Text：(word,1) ---> string
 *  IntWritable：这个是多少行 ---> int类型
 *
 * Mapper阶段：
 *  （1）用户自定义的Mapper要继承自己的父类
 *  （2）Mapper的输入数据是KV对的形式（KV的类型可以自定义）
 *  （3）Mapper中的业务逻辑写在map()方法中
 *  （4）Mapper的输出数据是KV对的形式（KV的类型可以自定义）
 *  （5）map()方法（MapTask进程）对每一个<K,V>调用一次
 *
 * 需求  --->  逻辑  ---> 代码
 * 需求：
 *  解决什么问题
 *
 * 逻辑：
 *  通过什么办法去解决
 *
 * 代码：
 *  具体解决办法
 *
 *@author: 情深@骚明
 *@time: 2021/2/3 17:06
 *@Version 1.0
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 防止垃圾回收过于频繁
    private static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * Ctrl + O ---> 实现业务逻辑
     * @param key
     * @param value
     * @param context Job任务 ---> 在框架下变成了任务，Map
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取数据
        String line = value.toString();

        // 按照空格切分数据
        String[] words = line.split(" ");

        // 遍历输出，把每个单词变成（word1,1）、（word2,1）、（word3,1）…… 交给框架
        for (String word :
                words) {
            // 官网是不建议使用这种代码编程方式
            // 我们处理是大数据量，大量的生成新的对象，内存有回收对象，垃圾回收周期变频繁，大量的去回收对象
            // context.write(new Text(word), new IntWritable(1));
            this.word.set(word);
            context.write(this.word,one);
        }
    }
}