package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.SpatBroadcastRateEvent;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

@Deprecated(since = "3.3.0", forRemoval = true)
@Slf4j
public class SpatZeroRateChecker
    extends BaseZeroRateChecker<ProcessedSpat, SpatBroadcastRateEvent, RsuIntersectionKey> {

    public SpatZeroRateChecker(int rollingPeriodSeconds, int outputIntervalSeconds, String inputTopicName, String stateStoreName) {
        super(rollingPeriodSeconds, outputIntervalSeconds, inputTopicName, stateStoreName);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected SpatBroadcastRateEvent createEvent() {
        return new SpatBroadcastRateEvent();
    }
}
