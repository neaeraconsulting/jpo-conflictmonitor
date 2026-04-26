package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MetricType {

    @JsonProperty("MetricID")
    private int metricID;

    @JsonProperty("ChartName")
    private String chartName;

    @JsonProperty("Abbreviation")
    private String abbreviation;

    @JsonProperty("ShowOnWebsite")
    private boolean showOnWebsite;

    @JsonProperty("ShowOnAggregationSite")
    private boolean showOnAggregationSite;

    @JsonProperty("DisplayOrder")
    private int displayOrder;
}

