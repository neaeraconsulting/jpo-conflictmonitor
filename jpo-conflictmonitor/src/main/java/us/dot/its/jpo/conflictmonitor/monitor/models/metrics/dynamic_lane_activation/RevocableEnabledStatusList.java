package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;

/**
 * A list of enabled/disabled statuses for one revocable lane
 */
@Data
@EqualsAndHashCode(callSuper=true)
public class RevocableEnabledStatusList extends ArrayList<RevocableEnabledStatus> {

    @Override
    public RevocableEnabledStatus getLast() {
        if (this.isEmpty()) {
            return null;
        }
        return super.getLast();
    }

}
