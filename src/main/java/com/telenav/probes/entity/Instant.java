package com.telenav.probes.entity;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Instant implements Serializable {
    private long timestamp;
    private double lat;
    private double lon;

    public Instant() { // default implementation ignored
    }

}
