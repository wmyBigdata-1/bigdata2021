package wmy.hadoop.mapreduce.writeTableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 *@description: 自定义实现排序
 *@author: 情深@骚明
 *@time: 2021/2/5 7:56
 *@Version 1.0
 */
public class flowBean implements WritableComparable<flowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public flowBean() {
    }

    /**
     * 将来包装数据的时候方便一点
     * 我们需要把这个Bean放在Hadoop的框架中，所以我们需要实现Hadoop的序列化
     * @param upFlow
     * @param downFlow
     */
    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }


    @Override
    public int compareTo(flowBean o) {
        return Long.compare(o.sumFlow,this.sumFlow);
    }

    /**
     * 注意：怎么样序列化就怎么样反序列，顺序一定要一致，否则的话可能会出现结果不一致的情况
     * 序列化 ---> 通过out写给我们的Hadoop框架
     * @param out 框架给我们提供的数据出口
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @param in 框架提供的数据来源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
}