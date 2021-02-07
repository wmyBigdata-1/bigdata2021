package hbase;
/*
 *Author：情深，骚明 and 情骚
 *Version：2019/12/20 & 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectionInstance;
import utils.HBaseUtils;
import utils.PropertiesUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAO {
    private int regions;
    private String nameSpace;
    private String tableName;
    private static final Configuration conf;
    private HTable table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    //用来优化插入数据用的，每30条插入一次
    private List<Put> cacheList = new ArrayList<>();

    static {
        conf = HBaseConfiguration.create();
    }


    public HBaseDAO() {
        try {
            nameSpace = PropertiesUtils.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtils.getProperty("hbase.calllog.tablename");
            regions = Integer.valueOf(PropertiesUtils.getProperty("hbase.calllog.regions"));

            if (HBaseUtils.isExistTable(conf, tableName)) {
                HBaseUtils.initNameSpace(conf, nameSpace);
                //f1：主叫，f2：存放被叫数据
                HBaseUtils.createTable(conf, tableName, regions,"f1","f2");
            }

        } catch (IOException e) {
            e.printStackTrace();

        }
    }


    /**
     * 数据样式
     * 14575535933,17526304161,2019-12-27 08:57:50,0229
     * @param value 一行数据
     */
    public void put(String value) throws ParseException, IOException {
        //数据优化、节省IO
        //每30行数据放一次
        if (cacheList.size() == 0) {
            try {
                connection = ConnectionInstance.getConnection(conf);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                //关闭自动连接
                table.setAutoFlush(false);
                table.setWriteBufferSize(2 * 1024 * 1024);
            } catch (IOException e) {
                e.printStackTrace();
            }

            //14575535933,17526304161,2019-12-27 08:57:50,0229
            String[] splitOri = value.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];

            String regionCode = HBaseUtils.getRegionCode(caller, buildTime, regions);

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));

            String buildTimeTS = String.valueOf(sdf1.parse(buildTime).getTime());

            //rowkey
            String rowkey = HBaseUtils.getRowkey(regionCode, caller, buildTimeReplace, callee, "1", duration);

            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTime_TS"), Bytes.toBytes(buildTimeTS));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            //优化
            cacheList.add(put);

            if (cacheList.size() >= 30) {
                table.put(cacheList);
                table.flushCommits();
                table.close();
                cacheList.clear();
            }
        }
    }
}
