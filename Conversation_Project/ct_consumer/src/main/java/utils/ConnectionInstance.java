package utils;/*
 *Author：情深，骚明 and 情骚
 *Version：2019/12/20 & 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class ConnectionInstance {
    private static Connection connection;

    public static synchronized Connection getConnection(Configuration conf) {
        if (conf == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return connection;

    }
}
