package com.telenav.probes.hbase;

import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.*;

public class HbaseConnectionsTest {

    @Test
    @SneakyThrows
    public void scanByPrefixFilter() {
        try(HbaseConnections hbaseConnections = new HbaseConnections()){
            hbaseConnections.scanByPrefixFilter("probe_test","bc6036b51c6d5c840511de1350acc8b5","info");
        }

    }
}