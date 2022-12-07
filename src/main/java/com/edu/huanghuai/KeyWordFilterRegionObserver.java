package com.edu.huanghuai;

import javafx.css.SimpleStyleableIntegerProperty;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import org.apache.hadoop.hbase.util.Bytes;

import java.awt.event.KeyEvent;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class KeyWordFilterRegionObserver implements RegionObserver , RegionCoprocessor {
    private  static final String ALIBABA="阿里巴巴";
    private  static final String MASK="**";
    
    
    public  void postGetOp(ObserverContext<RegionCoprocessorEnvironment>  observerContext, Get get, List<Cell> list)throws IOException{
        if(CollectionUtils.isNotEmpty(list)){
            for(int i=0;i<list.size();i++){
                Cell    cell=list.get(i);
                KeyValue keyValue=replaceCell(cell);
                list.set(i, (Cell) keyValue);
            }
        }
    }
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment>observerContext, InternalScanner internalScanner,List<Result> list,int i,boolean b)throws IOException{
        Iterator<Result> iterator=list.iterator();
        while(iterator.hasNext()){
            Result result=iterator.next();
            Cell[] cells=result.rawCells();
            if(CollectionUtils.isNotEmpty(list)){
                for(int j=0;j<cells.length;j++){
                    Cell cell=cells[j];
                    KeyValue keyValue=replaceCell(cell);
                    cells[j]= (Cell) keyValue;
                }
            }
        }
        return true;
    }
    private  KeyValue replaceCell(Cell cell){
        byte[] oldValueByte= CellUtil.cloneValue(cell);
        String oldValueByteString=Bytes.toString(oldValueByte);
        String newValueByteString=oldValueByteString.replaceAll(ALIBABA,MASK);
        byte[]  newValueByte= Bytes.toBytes(newValueByteString);
        return new KeyValue(CellUtil.cloneRow(cell),
                CellUtil.cloneFamily(cell),CellUtil.cloneQualifier(cell),
                cell.getTimestamp(),newValueByte); 
    }
}
