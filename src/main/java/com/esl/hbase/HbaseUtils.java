package com.esl.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtils {
    // 创建表
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        // 获取Hbase管理对象
        HBaseAdmin hBaseAdmin = HbaseConnect.gethBaseAdmin();
        if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
            // 先disabled再delete
            hBaseAdmin.disableTable(TableName.valueOf(tableName));
            hBaseAdmin.deleteTable(TableName.valueOf(tableName));
        }
        // HTableDescriptor类包含了表的名字以及表的类族信息
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf:
             columnFamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        hBaseAdmin.createTable(hTableDescriptor);
        hBaseAdmin.close();
    }

    public static void putsToHbase(String tableName, String rowKey, String cf, String[] columns, String[] values) throws IOException {
        // 连接到Hbase所对应的数据表
        Table table = HbaseConnect.getConnection().getTable(TableName.valueOf(tableName));
        // 插入数据的操作
        Put data = new Put(rowKey.getBytes());
        for (int i = 0; i < columns.length; i++) {
            data.addColumn(
                    Bytes.toBytes(cf),
                    Bytes.toBytes(columns[i]),
                    Bytes.toBytes(values[i])
            );
        }
        // 插入数据到表中
        table.put(data);
        table.close();
    }
}
