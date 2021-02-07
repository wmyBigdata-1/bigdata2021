package wmy.hadoop.mapreduce.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/3 20:51
 *@Version 1.0
 */
public class flowBean implements Writable {
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

    /**
     * 注意：怎么样序列化就怎么样反序列，顺序一定要一致，否则的话可能会出现结果不一致的情况
     * 序列化 ---> 通过out写给我们的Hadoop框架
     * @param dataOutput 框架给我们提供的数据出口
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @param dataInput 框架提供的数据来源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }
}