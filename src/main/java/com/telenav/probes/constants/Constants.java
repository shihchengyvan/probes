package com.telenav.probes.constants;

public class Constants {
    public static final double EARTH_RADIUS = 6371393; // metre

    // THRESHOLD
    public static final long TIME_INTERVAL_THRESHOLD = 30; // seconds
    public static final double SPEED_THRESHOLD = 80; // m/s

    // CONFIG
    public static final String PAYLOAD = "payload";
    public static final String LOG_CONTEXT = "log_context";
    public static final String CAR_ID = "car_id";
    public static final String UTC_TIMESTAMP = "utc_timestamp";
    public static final String PROBE_LIST = "probe_list";
    public static final String TIMESTAMP = "timestamp";
    public static final String LAT = "lat";
    public static final String LON = "lon";
    public static final long TIMESTAMP_20200701 = 1593561600L;

    private Constants(){
    }
}
