package us.dot.its.jpo.conflictmonitor.monitor.utils;

import org.apache.commons.lang3.builder.DiffBuilder;
import org.apache.commons.lang3.builder.DiffResult;
import org.apache.commons.lang3.builder.ReflectionDiffBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

import java.sql.Ref;

/**
 * Comparison methods for RTCM Message Count Progression Events
 */
public class RtcmDiff {

    /**
     * Comparison methods for RTCM Message Count Progression Events.
     * Compare everything other than the message count, timestamps, and metadata that isn't part of the
     * original ASN.1 message.
     * @param rtcmA The first RTCM
     * @param rtcmB A following RTCM
     * @return DiffResult with differences for each field
     */
    public DiffResult<ProcessedRTCM> compare(ProcessedRTCM rtcmA, ProcessedRTCM rtcmB) {
        var builder = new ReflectionDiffBuilder<ProcessedRTCM>(rtcmA, rtcmB, ToStringStyle.MULTI_LINE_STYLE)
                .setExcludeFieldNames("odeReceivedAt", "utcTime", "schemaVersion", "messageType", "originIp", "asn1",
                        "validationMessages", "cti4501Conformant");
        var result = builder.build();

    }

}
