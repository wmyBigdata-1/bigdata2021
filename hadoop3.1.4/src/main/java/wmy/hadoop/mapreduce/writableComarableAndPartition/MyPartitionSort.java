package wmy.hadoop.mapreduce.writableComarableAndPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import wmy.hadoop.mapreduce.writeTableComparable.flowBean;

/*
 *@description: 分区是在Mapper之后进行分区的，Reduce的数量决定分区的数量
 *@author: 情深@骚明
 *@time: 2021/2/5 8:46
 *@Version 1.0
 */
public class MyPartitionSort extends Partitioner<flowBean, Text> {
    @Override
    public int getPartition(flowBean flowBean, Text text, int numPartitions) {
        switch (text.toString().substring(0, 3)) {
            case "136": return 0;
            case "137": return 1;
            case "138": return 2;
            case "139": return 3;
            default:return 4;
        }
    }
}