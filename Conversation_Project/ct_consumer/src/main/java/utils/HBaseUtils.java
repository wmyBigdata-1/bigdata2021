package utils;/*
 *Author：情深，骚明 and 情骚
 *Version：2019/12/20 & 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * 命名空间、表、判断表、创建表、rowkey、列簇、列
 * region = 6
 * 协处理器、region是一个难点
 * 命名空间描述器、表描述器、列簇表述器
 */
public class HBaseUtils {
    //初始化namespace、表、rowkey、region
    //判断表是否存在

    /**
     * 初始化命名空间
     *
     * @param conf      配置
     * @param nameSpace 空间的名字
     */
    public static void initNameSpace(Configuration conf, String nameSpace) throws IOException {
        //创建connection对象
        Connection connection = ConnectionFactory.createConnection(conf);

        //获取admin
        Admin admin = connection.getAdmin();

        //命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor
                .create(nameSpace)
                //自定义配置，个性化设置
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .build();

        //创建
        admin.createNamespace(build);

        //关流
        admin.close();
        connection.close();

    }

    /**
     * 判断表是否存在
     * @param conf 配置
     * @param tableName 表名
     * @return 返回一个是否存在
     */
    public static boolean isExistTable(Configuration conf,String tableName) throws IOException {
        //创建connection对象
        Connection connection = ConnectionFactory.createConnection(conf);

        //获取admin
        Admin admin = connection.getAdmin();

        boolean b = admin.tableExists(TableName.valueOf(tableName));

        //关流
        admin.close();
        connection.close();

        return b;

    }



    /**
     * 创建表
     * @param conf 配置
     * @param tableName 表名
     * @param regions 多个rowkey
     * @param columnFamily 列簇
     */
    public static void createTable(Configuration conf, String tableName, int regions,
                                   String... columnFamily) throws IOException {
        //创建connection对象
        Connection connection = ConnectionFactory.createConnection(conf);

        //获取admin
        Admin admin = connection.getAdmin();

        if (isExistTable(conf,tableName)) return;

        //创建表描述器
        //不能之间放表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : columnFamily) {
            //列簇表述器
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }

        //创建表
        //不加此条，可以加入数据，数据可以写入，但是UI界面上不显示协处理器
        hTableDescriptor.addCoprocessor("hbase.CalleeWriteObserver");
        admin.createTable(hTableDescriptor, genSplitKey(regions));

        //关流
        admin.close();
        connection.close();
    }


    /**
     * 默认的会创建一个二维数组
     * 均匀的切分region
     * @param regions 表的个数
     * @return 二维数组
     */
    private static byte[][] genSplitKey(int regions) {
        String[] keys = new String[regions];
        //一个region默认是两个G
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            //为什么“|”，它是最大的一个值
            keys[i] = df.format(i) + "|";
        }

        byte[][] splitKeys = new byte[regions][];

        //排序
        TreeSet<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < regions; i++) {
            set.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> splitKeysIterator = set.iterator();
        int index = 0;
        while (splitKeysIterator.hasNext()) {
            byte[] next = splitKeysIterator.next();
            splitKeys[index++] = next;
        }

        return splitKeys;
    }

    /**
     * RW构成：regionCode_caller_buildTime_callee_flag_duration
     *
     * @param regionCode 分区号
     * @param caller     主叫
     * @param buildTime  通话建立时间
     * @param callee     被叫
     * @param flag       被叫标记
     * @param duration   持续时间
     * @return
     */
    public static String getRowkey(String regionCode, String caller, String buildTime, String callee, String flag, String duration) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(regionCode + "-")
                .append(caller + "-")
                .append(buildTime + "-")
                .append(callee + "-")
                .append(flag + "-")
                .append(duration);

        return buffer.toString();
    }

    /**
     * 足够离散
     *
     * @param caller    主叫
     * @param buildTime 建立通话时间
     * @param regions   表的个数
     * @return regionCode
     */
    public static String getRegionCode(String caller, String buildTime, int regions) {
        //主叫电话号码的长度
        int length = caller.length();

        //拿到主叫号码长度的后四位
        String lastPhone = caller.substring(length - 4);

        //年月 2019-10-24
        //201910
        String ym = buildTime.replaceAll("-", "").substring(0, 6);

        //离散操作
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);

        int y = x.hashCode();

        int regionCode = y % regions;

        DecimalFormat df = new DecimalFormat("00");

        return df.format(regionCode);
    }
}
