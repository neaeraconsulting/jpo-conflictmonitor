package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;

/**
 * Key to aggregate {@link PriorityRequestMetrics} on.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class IntersectionVehicleTypeKey {
    private Integer intersectionId;
    private Integer region;
    private ProcessedBasicVehicleRole vehicleType;

    @Override
    public String toString() {
        try {
            return DateJsonMapper.getInstance().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Exception serializing to JSON", e);
        }
        return "";
    }
}
