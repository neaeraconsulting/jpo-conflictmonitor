package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class DetectionType {

    @JsonProperty("DetectionTypeID")
    private int detectionTypeID;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("MetricTypes")
    private List<MetricType> metricTypes;
}

