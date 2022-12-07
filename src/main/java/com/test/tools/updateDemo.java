package com.test.tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.Scanner;

public class updateDemo {
    public static Configuration conf;

    static
    {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static boolean isExists(String tableName) throws IOException
    {
        Connection connection=ConnectionFactory.createConnection(conf);
        Admin admin=connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }
    public static void putCells(String tableName,String rowKey,String columnFamily,String coulmnName ,String value)throws  Exception
    {
        Connection connection= ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        if (!isExists(tableName))
        {
            System.out.println("表不存在");
            return;
        }else {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(coulmnName), Bytes.toBytes(value));
            table.put(put);
            System.out.println("插入成功");
            table.close();
        }
    }
    public static void getCells(String tableName,String rowKey,String columnFamliy,String columnName) throws IOException {
        Connection connection= ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get =new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamliy),Bytes.toBytes(columnName));
        get.readAllVersions();
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell:cells)
        {
            String value =new String(CellUtil.cloneValue(cell));
            System.out.println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)) + "\n" +
            "列族" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\n" +"列名："+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +"\n" + "列值："+value
            );
        }
        table.close();
    }
    public static void deleteCells(String tableName,String rowKey,String columnName,String columnFamily) throws  Exception
    {
        Connection connection= ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        if (!isExists(tableName))
        {
            System.out.println("表不存在");
        }else
        {
            System.out.println("请输入要删除数据的表"+tableName);
            System.out.println("请输入要删除的行键："+rowKey);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(columnName),Bytes.toBytes(columnFamily));
            delete.addColumns(Bytes.toBytes(columnName),Bytes.toBytes(columnFamily));
            table.delete(delete);
            System.out.println("删除成功");
            table.close();
        }
    }
    public static void scanTable(String tableName,String startRow,String stopRow)throws Exception
    {
        Connection connection= ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan =new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        ResultScanner scanner =table.getScanner(scan);
        for (Result result:scanner)
         {
            Cell[] cells=result.rawCells();
            for (Cell cell:cells)
            {
                System.out.print(new String((CellUtil.cloneRow(cell))+"-"+new String(CellUtil.cloneFamily(cell))
                +"-" +new String(CellUtil.cloneQualifier(cell))+"-"+new String(CellUtil.cloneValue(cell)))+"\t");
            }
            System.out.println();
         }
        table.close();
    }
    public static void filterScanTable(String tableName,String startRow,String stopRow,
                                       String columnName,String columnFamily,String value)throws Exception
    {
        Connection connection= ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan =new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        FilterList filterList = new FilterList();
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );
        filterList.addFilter(columnValueFilter);
        scan.setFilter(filterList);
        ResultScanner scanner =table.getScanner(scan);
        for (Result result:scanner)
        {
            Cell[] cells=result.rawCells();
            for (Cell cell:cells)
            {
                System.out.print(new String((CellUtil.cloneRow(cell))+"-"+new String(CellUtil.cloneFamily(cell))
                        +"-" +new String(CellUtil.cloneQualifier(cell))+"-"+new String(CellUtil.cloneValue(cell)))+"\t");
            }
            System.out.println();
        }
        table.close();
    }

    public static void main(String[] args) throws Exception{
        while (true)
        {
            System.out.println("请进行如下操作：1：插入数据 ||2：获取数据 || 3：删除数据 ||4：查看表中数据 || 5：对表中数据进行过滤：");
            Scanner scanner=new Scanner(System.in);
            int  c = scanner.nextInt();
            scanner.nextLine();
            if (c == 1)
            {
                System.out.println("请输入要插入数据的表名：");
                String tableName = scanner.nextLine();

                System.out.println("请输入键值：");
                String rowKey = scanner.nextLine();
                System.out.println("请输入列族：");
                String columnFamily = scanner.nextLine();
                System.out.println("请输入列名：");
                String coulmnName = scanner.nextLine();
                System.out.println("请输入列值：");
                String value = scanner.nextLine();
                updateDemo.putCells(tableName,rowKey,columnFamily,coulmnName,value);
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
                System.out.println("要查看数据的表名：");
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
            }
        }
    }
}
