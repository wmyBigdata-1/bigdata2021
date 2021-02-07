package wmy.hadoop.mapreduce.Wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 *@description: Map处理完毕之后然后Reduce在进行处理
 *  Text ---> string
 *  IntWritable ---> int
 *  Text ---> string
 *  IntWritable ---> int
 *
 * Map的输出是Reduce的输入，Reduce的输出是最终的输出
 *
 * Reducer阶段：
 *  （1）用户自定义的Reducer要继承自己的父类
 *  （2）Reducer的输入数据类型对应Mapper的输出数据类型，也是KV
 *  （3）Reducer的业务逻辑写在Reducer方法中
 *  （4）ReducerTask进程对每一组相同key，<key,value>调用一次reduce
 *@author: 情深@骚明
 *@time: 2021/2/3 17:06
 *@Version 1.0
 */
public class WcReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    private IntWritable total = new IntWritable();
    /**
     * Ctrl + O
     * (word,1) ---> 一组一组的数据
     *  word ---> 是没有顺序的，和文件的输入数据的顺序是一致的
     * @param key 进行分组，默认就按照key进行分组
     * @param values 很多个1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value :
                values) {
            sum += value.get();
        }
        total.set(sum);
        context.write(key,total);
    }
}