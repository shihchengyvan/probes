package com.telenav.probes;

import com.telenav.probes.entity.Instant;
import com.telenav.probes.entity.Section;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.locationtech.jts.geom.Coordinate;
import scala.Tuple2;

import java.util.*;

import static com.telenav.probes.constants.Constants.*;


public class App {

    public static void main(String[] args) {
        // hide spark log
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession ss = SparkSession.builder().appName("probes").master("local[*]").getOrCreate();

        // read raw data
        Dataset<Row> lines = ss.read().json("src/main/resources/raw/not_null.json");
        Dataset<Section> oneLineSections = lines.map(line -> {
            Row payload = line.getStruct(line.fieldIndex(PAYLOAD));
            Row logContext = payload.getStruct(payload.fieldIndex(LOG_CONTEXT));
            String carId = logContext.getString(logContext.fieldIndex(CAR_ID));
            if (StringUtils.isBlank(carId))
                return null;
            Section section = new Section();
            section.setCarId(carId);
            List<Row> probeList = payload.getList(payload.fieldIndex(PROBE_LIST));
            for (Row probe : probeList) {
                Instant instant = new Instant();
                long timestamp = probe.getLong(probe.fieldIndex(TIMESTAMP));
                if (timestamp <= TIMESTAMP_20200701) {
                    continue;
                } else {
                    instant.setTimestamp(timestamp);
                }
                instant.setLat(probe.getDouble(probe.fieldIndex(LAT)));
                instant.setLon(probe.getDouble(probe.fieldIndex(LON)));
                section.getInstantList().add(instant);
            }
            if (section.getInstantList().isEmpty())
                return null;
            return section;
        }, Encoders.bean(Section.class)).filter(Objects::nonNull);

        Dataset<Tuple2<String, Section>> oneCarSections = oneLineSections.groupByKey(Section::getCarId,
                Encoders.STRING())
                .reduceGroups((x, y) -> {
                    x.getInstantList().addAll(y.getInstantList());
                    return x;
                });

        Dataset<Section> cleanedSections = oneCarSections.flatMap(x -> {
            String carId = x._1;
            List<Section> sections = new ArrayList<>();
            List<Instant> allInstants = x._2.getInstantList();
            allInstants.sort(Comparator.comparingLong(Instant::getTimestamp));
            if (allInstants.size() <= 1) {
                sections.add(x._2);
                return sections.iterator();
            }

            Section tempSection = new Section();
            tempSection.setCarId(carId);
            tempSection.getInstantList().add(allInstants.get(0));
            Instant lastEffectiveInstant = allInstants.get(0);
            for (int i = 1; i < allInstants.size(); i++) {
                Instant currentInstant = allInstants.get(i);
                long interval = currentInstant.getTimestamp() - lastEffectiveInstant.getTimestamp();
                // split a section if time interval is too long
                if (interval > TIME_INTERVAL_THRESHOLD) {
                    sections.add(tempSection);
                    // initiate a new section
                    tempSection = new Section();
                    tempSection.setCarId(carId);
                    tempSection.getInstantList().add(currentInstant);
                    lastEffectiveInstant = currentInstant;
                } else if (interval != 0) {
                    // add or drop instants
                    double distance = (new Coordinate(currentInstant.getLon(), currentInstant.getLat())
                            .distance(new Coordinate(lastEffectiveInstant.getLon(), lastEffectiveInstant.getLat())))
                            / 180 * Math.PI * EARTH_RADIUS;
                    double intervalSpeed = distance / interval;
                    if (intervalSpeed <= SPEED_THRESHOLD) {
                        tempSection.getInstantList().add(currentInstant);
                        lastEffectiveInstant = currentInstant;
                    }
                }
            }
            if (!tempSection.getInstantList().isEmpty()) {
                sections.add(tempSection);
            }
            return sections.iterator();
        }, Encoders.bean(Section.class));


        // output
        Dataset<String> geojsons = getGeoJsonStringDataset(cleanedSections);
        geojsons.coalesce(1).write().mode(SaveMode.Overwrite).text("src/main/resources/output/");

//        getDetailsJsonStringDataset(cleanedSections).coalesce(1).write().mode(SaveMode.Overwrite).text("src/main/resources/output_detail/");
    }

    private static Dataset<String> getGeoJsonStringDataset(Dataset<Section> cleanedSections) {
        return cleanedSections.map(section -> {
            StringBuilder sb = new StringBuilder();
            sb.append(",{\"type\": \"LineString\", \"coordinates\": [");
            for (Instant instant : section.getInstantList()) {
                sb.append("[").append(instant.getLon()).append(",").append(instant.getLat()).append("],");
            }
            sb.deleteCharAt(sb.length() - 1); // remove the last comma
            sb.append("]}");

            return sb.toString();
        }, Encoders.STRING());
    }

    private static Dataset<String> getDetailsJsonStringDataset(Dataset<Section> cleanedSections) {
        return cleanedSections.map(section -> {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"type\": \"LineString\", \"coordinates\": [");
            for (Instant instant : section.getInstantList()) {
                sb.append("[").append(instant.getLon()).append(",").append(instant.getLat()).append("],");
            }
            sb.deleteCharAt(sb.length() - 1); // remove the last comma
            sb.append("]").append(", \"carId\": \"").append(section.getCarId()).append("\"");
            sb.append("}");
            return sb.toString();
        }, Encoders.STRING());
    }

}
