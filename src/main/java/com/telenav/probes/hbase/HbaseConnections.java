package com.telenav.probes.hbase;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * @program: JavaStudy
 * @description:
 * @author: Lin.wang
 * @create: 2020-09-16 14:07
 **/

@Slf4j
public class HbaseConnections implements Closeable {

    private Connection connectionInstance;

    public HbaseConnections() {
        this.setup();
    }

    public HbaseConnections(String zookeeper) {
        this.setup(zookeeper);
    }


    private void setup() {
        setup("xid-gemini-03,xid-gemini-02");
    }

    @SneakyThrows
    public void setup(String zooKeeper) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        log.info("---------------  initial connection Hbase -----------------");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zooKeeper);
        connectionInstance = ConnectionFactory.createConnection(conf);
        log.info("---------------  connection Hbase finished  -----------------");
        log.info("table list：" + Arrays.toString(connectionInstance.getAdmin().listTableNames()));
    }

    @Override
    public void close() throws IOException {
        if (isValidConnection()) {
            log.info("---------------  close Hbase -----------------");
            connectionInstance.close();
        }
    }

    @SneakyThrows
    public void createTable(String myTableName, String[] colFamily) {
        if (!isValidConnection()) {
            log.error("create table failed due to failed connecting to Hbase.");
        }
        Admin admin = connectionInstance.getAdmin();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            log.info("table is exists!");
        } else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for (String family : colFamily) {
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptor.setColumnFamily(familyDescriptor);
            }
            admin.createTable(tableDescriptor.build());
        }
    }

    @SneakyThrows
    public void insertData(String tableName, String rowKey, String colFamily, String col, String val) {
        if (!isValidConnection()) {
            log.error("insert data failed due to failed connecting to Hbase.");
        }
        try (Table table = connectionInstance.getTable(TableName.valueOf(tableName))) {
            Put data = new Put(rowKey.getBytes());
            data.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
            table.put(data);
        }
    }

    @SneakyThrows
    public Table getTable(String tableName) {
        if (!isValidConnection()) {
            log.error("insert data failed due to failed connecting to Hbase.");
        }
        return connectionInstance.getTable(TableName.valueOf(tableName));
    }

    @SneakyThrows
    public void closeTable(Table table) {
        if (!isValidConnection()) {
            log.error("insert data failed due to failed connecting to Hbase.");
        }
        if (Objects.nonNull(table)) {
            table.close();
        }
    }

    @SneakyThrows
    public void insertDataWithExistTable(Table table, String rowKey, String colFamily, String col, String val) {
        if (Objects.nonNull(connectionInstance) && Objects.nonNull(table)) {
            Put data = new Put(rowKey.getBytes());
            data.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
            table.put(data);
        } else {
            log.error("insert table failed due to connection is null or table is null !");
        }
    }

    public String getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        if (!isValidConnection()) {
            log.error("get data failed due to failed connecting to Hbase.");
        }
        String dataResult;
        try (Table table = connectionInstance.getTable(TableName.valueOf(tableName))) {
            Get data = new Get(rowKey.getBytes());
            data.addColumn(colFamily.getBytes(), col.getBytes());
            Result result = table.get(data);
            dataResult = new String(result.getValue(colFamily.getBytes(), col.getBytes()));
        }
        return dataResult;
    }

    public void scanByRowPrefixFilter(String tableName, String rowPrefix, String family) {
        if (!isValidConnection()) {
            log.error("scan data failed due to failed connecting to Hbase.");
        }
        try (Table table = connectionInstance.getTable(TableName.valueOf(tableName))) {
            Scan scanDetails = new Scan();
            scanDetails.setFilter(new PrefixFilter(rowPrefix.getBytes()));
            ResultScanner resultScanner = table.getScanner(scanDetails);
            for (Result result : resultScanner) {
                log.info("row -> " + new String(result.getRow()));
                result.getFamilyMap(family.getBytes()).forEach((x, y) -> log.info(new String(x) + " : " + new String(y)));
            }
        } catch (IOException e) {
            log.error("connect to Hbase failed !", e);
        }
    }

    @SneakyThrows
    public void scanByRowRegexFilter(String tableName, String rowReg, String family) {
        if (!isValidConnection()) {
            log.error("scan data failed due to failed connecting to Hbase.");
        }
        try (Table table = connectionInstance.getTable(TableName.valueOf(tableName))) {
            Scan scanDetails = new Scan();
            Filter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(rowReg));
            scanDetails.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scanDetails);
            for (Result result : resultScanner) {
                log.info("row -> " + new String(result.getRow()));
                result.getFamilyMap(family.getBytes()).forEach((x, y) -> log.info(new String(x) + " : " + new String(y)));
            }
        } catch (IOException e) {
            log.error("connect to Hbase failed !", e);
        }
    }

    private boolean isValidConnection() {
        return connectionInstance != null && !connectionInstance.isClosed();
    }
}
