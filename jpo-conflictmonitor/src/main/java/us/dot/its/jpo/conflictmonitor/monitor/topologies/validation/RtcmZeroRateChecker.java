package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.RtcmBroadcastRateEvent;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

@Slf4j
public class RtcmZeroRateChecker
    extends BaseZeroRateChecker<ProcessedRTCM, RtcmBroadcastRateEvent, RsuStationIdKey> {


    public RtcmZeroRateChecker(int rollingPeriodSeconds, int outputIntervalSeconds, String inputTopicName, String stateStoreName) {
        super(rollingPeriodSeconds, outputIntervalSeconds, inputTopicName, stateStoreName);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected RtcmBroadcastRateEvent createEvent() {
        return new RtcmBroadcastRateEvent();
    }
}
