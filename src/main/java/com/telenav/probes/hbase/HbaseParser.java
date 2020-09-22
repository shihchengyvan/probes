package com.telenav.probes.hbase;

import com.google.gson.Gson;
import com.telenav.probes.entity.HbaseData;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Random;

/**
 * @program: probes
 * @description:
 * @author: Lin.wang
 * @create: 2020-09-21 17:20
 **/

@Getter
@RequiredArgsConstructor
public class HbaseParser {

    private HbaseData parserString2HbaseData(String data) {
        Gson gson = new Gson();
        return gson.fromJson(data, HbaseData.class);
    }

    @SneakyThrows
    public void insertData2Hbase(String path) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            Random random = SecureRandom.getInstanceStrong();
            String line = reader.readLine();
            try (HbaseConnections connections = new HbaseConnections()) {
                Table table = connections.getTable("probe_test");
                while (Objects.nonNull(line)) {
                    HbaseData data = parserString2HbaseData(line);
                    String col = String.join(",", new Timestamp(System.currentTimeMillis()).toString(), String.valueOf(random.nextLong()));
                    connections.insertTableWithExistTable(table, data.getCarId(), "info", col, line);
                    line = reader.readLine();
                }
                table.close();
            }
        }

    }

}
