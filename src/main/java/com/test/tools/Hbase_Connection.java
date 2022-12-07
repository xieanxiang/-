package com.test.tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase_Connection {
    private static Connection connection=null;
    static
    {
        createConnection();
    }
    private static synchronized void createConnection()
    {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","master,master02,slave01");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        System.out.println("连接成功");
        try
        {
            Connection conn= ConnectionFactory.createConnection(conf);
            System.out.println(conn);
        }catch (IOException ioException)
        {
         throw new RuntimeException(ioException);
        }
    }
}

