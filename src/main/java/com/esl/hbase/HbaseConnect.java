package com.esl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HbaseConnect {
    private static Connection connection;   // 连接
    private static HBaseAdmin hBaseAdmin;   // 操作对象
    static{
        // Hbase基于zookeeper整合
        // 配置
        Configuration conf = HBaseConfiguration.create();
        // 配置zookeeper的集群地址
        conf.set("hbase.zookeeper.quorum", "spark01,spark02,spark03");
        // zookeeper的端口号
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try{
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseAdmin gethBaseAdmin() {
        // 获取Hbase的数据库操作对象
        try{
            hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hBaseAdmin;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static synchronized void closeConnection() {
        if (connection != null){
            try {
                Admin admin = connection.getAdmin();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
