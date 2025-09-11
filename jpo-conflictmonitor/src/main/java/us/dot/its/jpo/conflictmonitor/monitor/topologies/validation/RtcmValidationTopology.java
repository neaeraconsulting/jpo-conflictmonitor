package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationStreamsAlgorithm;

/**
 * Assessments/validations for RTCM messages.
 */
@Slf4j
public class RtcmValidationTopology
    extends BaseValidationTopology<RtcmValidationParameters>
    implements RtcmValidationStreamsAlgorithm {

    // TODO Aggregation algorithm here

    @Override
    protected Logger getLogger() {
        return log;
    }



    @Override
    public Topology buildTopology() {
        return null;
    }
}
