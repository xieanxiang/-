package com.test.tools;

public class adasd {
    public static void Excel_to_HBase(String filePath,String keyName) throws IOException {
        HBaseHelper.Exec_CreateNamesplace(tableName);
        HBaseHelper.Exec_CreateTable(tableName, family);
        if(!filePath.endsWith(".xlsx")){
            System.out.println("传入文件不是excel类型");
        }
        FileInputStream fis = null;
        Workbook workbook = null;

        //获取绝对路径
        try{
            fis = new FileInputStream(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //创建工作表
        try{
            //XSSF读取后缀为xlsx的excel文件
            assert fis != null;
            workbook = new XSSFWorkbook(fis);
        }catch (Exception e){
            try{
                //HSSF读取后缀为xls的excel文件
                workbook = new HSSFWorkbook(fis);
            }catch (Exception ex){
                ex.printStackTrace();
            }

        }

        //得到一个工作表
        assert workbook != null;
        Sheet sheet = workbook.getSheetAt(0);
        //获取表头
        Row rowHead = sheet.getRow(0);
        //获取总列数
        int totalColumnNum = rowHead.getPhysicalNumberOfCells();
        //获取总行数，这里会因为excel格式的问题而获取与实际数据不一致的行数信息，获取的都是比实际行数大的值
        int totalRowNum = sheet.getLastRowNum();

        //获取keyName所在列的列数
        int rowIndex = 0;
        for (int i = 0; i < totalColumnNum; i++) {
            Cell sheetTableName = rowHead.getCell(i);
            if(String.valueOf(sheetTableName).equals(keyName)){
                rowIndex = i;
            }
        }
        //获取excel实际长度
        int ListLength = 0;
        for (int i = 0; i < totalRowNum; i++) {
            Row row = sheet.getRow(i+1);
            Cell cell1 = row.getCell(rowIndex);
            //指定格式，否则无法读取数字列
            cell1.setCellType(CellType.STRING);
            String data = cell1.getStringCellValue();
            if (!data.isEmpty()){
                ListLength = i;
            }
        }
        //循环遍历添加数据
        for (int i = 0; i < totalColumnNum; i++) {
            //不添加作为行键的数据列
            if (i!=rowIndex) {
                Cell cell = rowHead.getCell(i);
                if(!String.valueOf(cell).isEmpty()) {
                    System.out.println(cell);
                    //判断是否为空
                    if (!String.valueOf(cell).isEmpty()) {
                        for (int j = 0; j <= ListLength; j++) {
                            Row row = sheet.getRow(j + 1);
                            Cell cell1 = row.getCell(i);
                            Cell key = row.getCell(rowIndex);
                            //指定格式，否则无法读取数字列
                            cell1.setCellType(CellType.STRING);
                            String data = cell1.getStringCellValue();
                            //调用函数
                            Exec_AddData(tableName, family, String.valueOf(key), String.valueOf(cell), data);
                        }
                    }
                }
            }
        }
    }
}
    //插入数据
    public static boolean Exec_AddData(String Tname,String family,String rowkey,String column,String value){
        try {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            TableName tableName = TableName.valueOf(Tname);
            GetConn().getTable(tableName).put(put);
            return true;
        } catch (IOException e) {
            return false;
        }

    }
