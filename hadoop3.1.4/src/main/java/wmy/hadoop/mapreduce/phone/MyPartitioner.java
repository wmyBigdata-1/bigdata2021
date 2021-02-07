package wmy.hadoop.mapreduce.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import wmy.hadoop.mapreduce.flow.flowBean;

/*
 *@description: 数据类型是从Mapper出来的
 *@author: 情深@骚明
 *@time: 2021/2/4 11:55
 *@Version 1.0
 */
public class MyPartitioner extends Partitioner<Text, flowBean> {
    @Override
    public int getPartition(Text text, flowBean flowBean, int numPartitions) {
        // 获取数据
        String phone = text.toString();
        switch (phone.substring(0, 3)) { // jdk1.8之后的特性，maven的默认版本是1.5
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;

        }
    }
}