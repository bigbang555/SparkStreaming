package com.imooc.spark.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Horizon
 * Time: 下午10:14 2018/2/24
 * Description:
 */
public class HBaseUtils {
    private HBaseAdmin admin;
    private Configuration configuration;
    private HBaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","hadoop00:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop00:9000/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public synchronized static HBaseUtils getInstance(){
        if(instance==null){
            instance = new HBaseUtils();
        }
        return instance;
    }

    public HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName,String rowKey,String cf,String column,String value){
        HTableDescriptor descriptor = new HTableDescriptor();
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String tableName = "imooc_cource_clickcount";
        String rowKey = "20180224_131";
        String cf = "info";
        String column = "click_count";
        String value = "3";

        HBaseUtils.getInstance().put(tableName,rowKey,cf,column,value);
    }

}
