package com.test.tools;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableDemo {
    public static Configuration conf;
    static {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","master,master02,slave02");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static boolean isExists(String tableName) throws IOException {
        if(tableName.equals("")){
            System.out.println("表名不能为空!");
            return false;
        }

        Connection connection=ConnectionFactory.createConnection(conf);
        Admin admin=connection.getAdmin();
//        admin.close();
        return admin.tableExists(TableName.valueOf(tableName));
    }
    public static void createTable(String tableName,String[] columnFamilies)throws Exception
    {

        Connection connection=ConnectionFactory.createConnection(conf);
        Admin admin=connection.getAdmin();
        if (isExists(tableName))
        {
            System.out.println("表已存在");
        }else
        {
            TableDescriptorBuilder tableDescriptorBuilder =  TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            for (String  columnFamiy: columnFamilies)
            {
                System.out.println("列族："+columnFamilies);
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder=ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(columnFamiy));
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("创建成功");
            admin.close();
        }
    }
    public static void deleteTable(String tableName) throws Exception
    {
        Connection connection=ConnectionFactory.createConnection(conf);
        Admin admin=connection.getAdmin();
        if (isExists(tableName))
        {
            if(!admin.isTableDisabled(TableName.valueOf(tableName))){
                admin.disableTable(TableName.valueOf(tableName));
            }

            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表删除成功");
        }else
        {
            System.out.println("表不存在");
        }
        admin.close();
    }
}
