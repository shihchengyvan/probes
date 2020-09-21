package com.telenav.probes.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Section {
    private String carId = "";
    private String timeZone = "";
    List<Instant> instantList = new ArrayList<>();

    public Section() { // default implementation ignored
    }
}
