package wmy.hadoop.mapreduce.reduuceJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import wmy.hadoop.mapreduce.reduuceJoin.bean.OrderBean;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/7 8:37
 *@Version 1.0
 */
public class JoinMapperAfterComparator extends WritableComparator {
    public JoinMapperAfterComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}