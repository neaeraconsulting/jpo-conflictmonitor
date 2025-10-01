package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IntersectionVehicleTypeKey {
    private Integer intersectionId;
    private Integer region;
    private ProcessedBasicVehicleRole vehicleType;
}
