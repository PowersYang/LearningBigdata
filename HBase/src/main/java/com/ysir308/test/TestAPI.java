package com.ysir308.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL
 * 1、创建命名空间
 * 2、表的增删
 * <p>
 * <p>
 * DML
 * 数据的增删改查
 */
public class TestAPI {

    private static Connection conn = null;
    private static Admin admin = null;

    static {
        try {

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        return exists;
    }

    /**
     * 创建表
     *
     * @param tableName 表名称
     * @param cfs       列族
     */
    public static void createTable(String tableName, String... cfs) throws IOException {

        // 判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }

        // 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + " 表已存在！");
            return;
        }

        // 表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 循环添加列族信息
        for (String cf : cfs) {
            // 列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            // 添加列族
            hTableDescriptor.addFamily(hColumnDescriptor);
        }


        // 创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {

        if (!isTableExist(tableName)) {
            System.out.println(tableName + " 表不存在！");
        }

        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建命名空间
     *
     * @param namespace
     */
    public static void createNamespace(String namespace) {

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

        try {

            admin.createNamespace(namespaceDescriptor);

        } catch (NamespaceExistException e) {
            // API没有提供专门的方法来判断namespace是否已经存在
            // 但是可以通过捕获异常来解决

            System.out.println(namespace + " 命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param cf        列族
     * @param cn        列
     * @param value     值
     */
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        // 添加多个列，如果需要添加多个rowKey的话就需要构造多个Put
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        table.put(put);

        table.close();
    }

    /**
     * 获取数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     */
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        get.setMaxVersions(5);

        // 查询结果集
        Result result = table.get(get);

        if (!result.isEmpty()) {
            for (Cell cell : result.rawCells()) {
                System.out.println("-------------------------");
                System.out.println("列族： " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列： " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值： " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
    }

    /**
     * 扫描全表
     *
     * @param tableName
     * @throws IOException
     */
    public static void scanTable(String tableName) throws IOException {

        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("-------------------------");
                System.out.println("RowKey： " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族： " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列： " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值： " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
    }


    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws IOException
     */
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        Table table = conn.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 在生产环境中尽量使用addColumns()方法，addColumn()方法慎用
        // 不然会出现一些很诡异的现象
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));

        table.delete(delete);

        table.close();
    }

    public static void close() {

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws IOException {

        System.out.println(isTableExist("student"));

        createTable("student", "info1", "info2");

        close();
    }
}
