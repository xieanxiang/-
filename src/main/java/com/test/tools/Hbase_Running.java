package com.test.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

import static org.apache.zookeeper.ZooDefs.OpCode.delete;

public class Hbase_Running {
        public static Configuration conf;

    static
    {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
        public static void putCells(String tableName,String rowKey,String columnFamily,String columnName ,String value)throws  Exception
        {
            Connection connection= ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            /*if (!isExists(tableName))
            {
                System.out.println("表不存在");
                return;
            }*/

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
            System.out.println("插入成功");
            table.close();

        }

    public static void main(String[] args) throws Exception{
        Hbase_Connection hbase_connection=new Hbase_Connection();
        System.out.println("连接成功");
        Scanner scanner=new Scanner(System.in);
        System.out.println("请进行如下操作：1：表管理 ||  2:表操作");
        int a=scanner.nextInt();
        if (a==1)
        {
            System.out.println("表管理如下：1：创建表 || 2:删除表");
            int  b=scanner.nextInt();
            scanner.nextLine();
            if (b == 1) {
                System.out.print("请输入表名：");
                String tableName = scanner.nextLine();
                System.out.println(TableDemo.isExists(tableName));
                System.out.print("请输入列族：");
                String cf = scanner.nextLine();
                TableDemo.createTable(tableName,new String[] {cf});
            }
            if (b == 2)
            {
                System.out.println("请输入要删除的表名：");
                String tableName=scanner.nextLine();
                System.out.println(TableDemo.isExists(tableName));
                TableDemo.deleteTable(tableName);
            }else
            {
                System.out.println("操作出错");
            }
        }
        if (a == 2)
        {
            System.out.println("请进行如下操作：1：插入数据 ||2：获取数据 || 3：删除数据 ||4：查看表中数据 || 5：对表中数据进行过滤：");
            int  c = scanner.nextInt();
            scanner.nextLine();
            if (c == 1)
            {

                System.out.println("请输入要插入数据的表名：");
                String tableName = scanner.nextLine();
                System.out.println(TableDemo.isExists(tableName));
                System.out.println("请输入键值：");
                String rowKey = scanner.nextLine();
                System.out.println("请输入列族：");
                String columnFamily = scanner.nextLine();
                System.out.println("请输入列名：");
                String coulmnName = scanner.nextLine();
                System.out.println("请输入列值：");
                String value = scanner.nextLine();
                //Put put = new Put(Bytes.toBytes(rowKey));
                // updateDemo.putCells(put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(coulmnName),Bytes.toBytes(value)));
                //updateDemo.putCells.(rowKey,tableName,columnFamily,coulmnName,value);

            }
            if (c == 2)
            {
                System.out.println("请输入要获取数据的表名：");
                String tableName = scanner.nextLine();
                System.out.println("请输入键值：");
                String rowKey = scanner.nextLine();
                System.out.println("列族：");
                String columnFamily = scanner.nextLine();
                System.out.println("列名：");
                String coulmnName = scanner.nextLine();
                updateDemo.getCells(tableName,rowKey,columnFamily,coulmnName);
            }
            if (c == 3)
            {
                System.out.println("要删除数据的表名：");
                String tableName = scanner.nextLine();
                System.out.println(TableDemo.isExists(tableName));
                System.out.println("请输入键值,列族，列名：");
                String rowKey = scanner.nextLine();
                String columnFamily = scanner.nextLine();
                String columnName = scanner.nextLine();
                updateDemo.deleteCells(tableName,rowKey,columnFamily,columnName);
            }
            if (c == 4)
            {
                System.out.println("要扫描数据的表名：");
                String tableName = scanner.nextLine();
                System.out.println("请输入扫描键值的起始位置：");
                String startRow = scanner.nextLine();
                String stopRow = scanner.nextLine();
                updateDemo.scanTable(tableName,startRow,stopRow);
            }
            if (c == 5)
            {
                System.out.println("要过滤数据的表名：");
                String tableName = scanner.nextLine();
                System.out.println("请输入扫描键值的起始位置：");
                String startRow = scanner.nextLine();
                String stopRow = scanner.nextLine();
                System.out.println("请输入列族：");
                String columnFamily = scanner.nextLine();
                System.out.println("请输入列名：");
                String coulmnName = scanner.nextLine();
                System.out.println("请输入列值：");
                String value = scanner.nextLine();
                updateDemo.filterScanTable(tableName,startRow,stopRow,columnFamily,coulmnName,value);
            }else
            {
                System.out.println("操作错误");
            }
        }

        //scanner.nextLine();
        System.out.print("请输入要插入数据的表名：");
        String tableName = scanner.next();
        //System.out.println(TableDemo.isExists(tableName));
        System.out.println("请输入键值：");
        String rowKey = scanner.next();
        System.out.println("请输入列族：");
        String columnFamily = scanner.next();
        System.out.println("请输入列名：");
        String coulmnName = scanner.next();
        System.out.println("请输入列值：");
        String value = scanner.next();
        putCells(tableName,rowKey,columnFamily,coulmnName,value);






    }
}
