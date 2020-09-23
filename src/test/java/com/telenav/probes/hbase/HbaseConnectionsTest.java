package com.telenav.probes.hbase;

import lombok.SneakyThrows;
import org.junit.Test;

public class HbaseConnectionsTest {

    @Test
    @SneakyThrows
    public void scanByRowPrefixFilter() {
        try(HbaseConnections hbaseConnections = new HbaseConnections()){
            hbaseConnections.scanByRowPrefixFilter("probe_test","bc6036b51c6d5c840511de1350acc8b5","info");
        }
    }

    @Test
    @SneakyThrows
    public void scanByRowRegexFilterCarID() {
        try(HbaseConnections hbaseConnections = new HbaseConnections()){
            hbaseConnections.scanByRowRegexFilter("probe_test","^bc.*","info");
        }
    }
    @Test
    @SneakyThrows
    public void scanByRowRegexFilterGeoHash() {
        try(HbaseConnections hbaseConnections = new HbaseConnections()){
            hbaseConnections.scanByRowRegexFilter("probe_test",".*_c22.*","info");
        }
    }
}