package com.thirdlucky.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class TestApi {
    private static Connection connection;
    private static Admin admin;

    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");

        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableContains(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    public static void createTable(String tableName, String... cfs) throws IOException {
        if (cfs.length == 0) {
            return;
        }

        if (isTableContains(tableName)) {
            return;
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
        }

        admin.createTable(tableDescriptor);
    }

    public static void dropTable(String tableName) throws IOException {

        if (!isTableContains(tableName)) {
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public static void createNS(String ns) {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(ns);
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteNS(String ns) {
        try {
            admin.deleteNamespace(ns);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void put(String tName, String rowKey, String cf, String cn, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tName));
        table.put(new Put(Bytes.toBytes(rowKey)).addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value)));
        table.close();
    }

    public static void get(String tName, String rowKey, String cf, String cn) throws IOException {
        System.out.println("get==========");
        Table table = connection.getTable(TableName.valueOf(tName));
        Result result = table.get(new Get(Bytes.toBytes(rowKey)).addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn)));

        printResult(result);
        table.close();
    }

    private static void printResult(Result result){
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.printf("RAW=%s,CF=%s,CN=%s,V=%s\n",
                    Bytes.toString(CellUtil.cloneRow(cell)),
                    Bytes.toString(CellUtil.cloneFamily(cell)),
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void scan(String tName) throws IOException {
        System.out.println("scan============");
        Table table = connection.getTable(TableName.valueOf(tName));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        scanner.forEach(r->{
            printResult(r);
        });

        table.close();
    }


    public static void delete(String tName, String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除最新的版本(慎用)
       // delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        // 删除所有版本
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
        // 删除时间戳的
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),1584535702783L);
        table.delete(delete);
        table.close();
    }

    public static void test() throws IOException {
        TableName tableName = TableName.valueOf("102:test");

        Table table = connection.getTable(tableName);
        table.put(new Put(Bytes.toBytes("1009test")).addColumn(Bytes.toBytes("info1"), Bytes.toBytes("cn"), Bytes.toBytes("value")));
        table.put(new Put(Bytes.toBytes("1009test")).addColumn(Bytes.toBytes("info1"), Bytes.toBytes("cn"), Bytes.toBytes("value1")));
        Delete delete = new Delete(Bytes.toBytes("1009test"));
        //        // 删除最新的版本(慎用)
         delete.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("cn"));

        table.delete(delete);
        table.close();
    }

    public static void main(String... args) throws IOException {
//        System.out.println(isTableContains("abc"));
//        System.out.println(isTableContains("stu"));
//
//
//        System.out.println(isTableContains("test"));
//
//        System.out.println("创建表");
//        createTable("test", "info1", "info2");
//        System.out.println(isTableContains("test"));
//
//        System.out.println("删除表");
//        dropTable("test");
//        System.out.println(isTableContains("test"));
//
//        createNS("102");
//        createTable("102:test", "info1", "info2");

//        put("102:test", "1001", "info1", "name", "zhangsan");
//        get("102:test", "1001", "info1", "name");
//        scan("102:test");

//        delete("102:test","1001","info1","name");
        test();
        close();
    }

    public static void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
