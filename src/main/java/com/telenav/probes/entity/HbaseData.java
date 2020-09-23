package com.telenav.probes.entity;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @program: probes
 * @description:
 * @author: Lin.wang
 * @create: 2020-09-21 17:12
 **/
@Setter
@Getter
public class HbaseData {
    @SerializedName("type")
    String type;
    @SerializedName("coordinates")
    List<List<String>> coordinates;
    @SerializedName("carId")
    String carId;
}
