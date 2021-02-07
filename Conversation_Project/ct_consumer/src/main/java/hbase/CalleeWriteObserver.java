package hbase;/*
 *Author：情深，骚明 and 情骚
 *Version：2019/12/21 & 1.0
 */

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtils;
import utils.PropertiesUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Ctrl + alt + O 快速删除无用的包
 */
public class CalleeWriteObserver extends BaseRegionObserver {

    //格式化数据
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    //PostPut
    //ctrl + alt + o 清理没有用的包
    //快速找继承的ctrl +o
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //super.postPut(e, put, edit, durability);
        //首先要获得操作的表
        String targetTableName = PropertiesUtils.getProperty("hbase.calllog.tablename");

        //获取当前put成功的表
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();

        //判断是否是目标表
        if (!targetTableName.equals(currentTableName)) return;

        //拿到数据
        String oriRowkey = Bytes.toString(put.getRow());

        //RW构成：regionCode_caller_buildTime_callee_flag_duration
        String[] splitOriRowkey = oriRowkey.split("_");

        //拿到每一位数字
        String oldFlag = splitOriRowkey[4];

        //插入的数据flag都是1，而不是0
        if (oldFlag.equals("0")) return;

        //表的个数
        Integer regions = Integer.valueOf(PropertiesUtils.getProperty("hbase.calllog.regions"));


        String caller = splitOriRowkey[1];
        String buildTime = splitOriRowkey[2];
        String callee = splitOriRowkey[3];
        String flag = "0";
        String duration = splitOriRowkey[5];

        String regionCode = HBaseUtils.getRegionCode(callee, buildTime, regions);

        //callee的rowkey
        String calleeRowkey = HBaseUtils.getRowkey(regionCode, callee, buildTime, caller, flag, duration);

        //时间戳
        String buildTimeTs = "";
        try {
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        //放数据
        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("caller"),Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("callee"),Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("flag"),Bytes.toBytes("0"));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildTime_TS"),Bytes.toBytes(buildTimeTs));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();

    }
}
