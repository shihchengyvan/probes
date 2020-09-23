package com.telenav.probes.hbase;

import com.google.gson.Gson;
import com.telenav.probes.entity.HbaseData;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Table;
import org.locationtech.jts.geom.Coordinate;
import ch.hsr.geohash.GeoHash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.util.List;
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
                    List<String> pointList = data.getCoordinates().get(data.getCoordinates().size() / 2);
                    Coordinate coordinate = new Coordinate(Double.valueOf(pointList.get(0)), Double.valueOf(pointList.get(1)));
                    String geohash = GeoHash.geoHashStringWithCharacterPrecision(coordinate.y, coordinate.x, 5);
                    // with key format with carID_geohash
                    connections.insertTableWithExistTable(table, String.join("_", data.getCarId(), geohash), "info", col, line);
                    line = reader.readLine();
                }
                table.close();
            }
        }

    }

}
