package wmy.hadoop.mapreduce.reduuceJoin.bean;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 *@description:
 *@author: 情深@骚明
 *@time: 2021/2/7 8:22
 *@Version 1.0
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String id;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public String toString() {
        return id + "\t" + pid + "\t" + amount + "\t" + pname;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public int compareTo(OrderBean o) {
        int compare = this.pid.compareTo(o.pid);
        if (compare == 0) {
            return o.pname.compareTo(this.pname);
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
    }
}