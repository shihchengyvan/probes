package com.telenav.probes.hbase;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.security.SecureRandom;
import java.sql.Timestamp;
import java.util.Random;

import static org.junit.Assert.*;

@Slf4j
public class HbaseParserTest {

    @Test
    @SneakyThrows
    public void timeStampTest() {
        Random random = SecureRandom.getInstanceStrong();
        log.info(String.join(",", new Timestamp(System.currentTimeMillis()).toString(), String.valueOf(random.nextLong())));
        log.info(String.join(",", new Timestamp(System.currentTimeMillis()).toString(), String.valueOf(random.nextLong())));
        log.info(String.join(",", new Timestamp(System.currentTimeMillis()).toString(), String.valueOf(random.nextLong())));
    }

    @Test
    @SneakyThrows
    public void createProbeDb() {
        try (HbaseConnections hbaseConnections = new HbaseConnections()) {
            hbaseConnections.createTable("probe_test", new String[]{"info"});
        }
    }

    @Test
    public void insertData(){
        HbaseParser hbaseParser = new HbaseParser();
        hbaseParser.insertData2Hbase("src/test/resources/probe_hbase/hbase.txt");
    }

}