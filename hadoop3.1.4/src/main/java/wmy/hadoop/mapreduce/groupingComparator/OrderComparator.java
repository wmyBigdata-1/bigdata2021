package wmy.hadoop.mapreduce.groupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 *@description: 分组完之后进行Reduce
 *@author: 情深@骚明
 *@time: 2021/2/6 8:31
 *@Version 1.0
 */
public class OrderComparator extends WritableComparator {
    public OrderComparator(){
        super(OrderBean.class,true); //
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}