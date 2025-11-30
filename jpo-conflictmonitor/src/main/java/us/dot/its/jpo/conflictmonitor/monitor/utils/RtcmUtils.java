package us.dot.its.jpo.conflictmonitor.monitor.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.DiffResult;
import org.apache.commons.lang3.builder.ReflectionDiffBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

import java.util.Set;

@Slf4j
public class RtcmUtils {

    /**
     * Checks for Multiple Signal Messages (MSM) RTCM types, which are 1070-1129
     * Ref. <a href="https://www.use-snip.com/kb/knowledge-base/an-rtcm-message-cheat-sheet/">an-rtcm-message-cheat-sheet</a>
     * @param rtcm The ProcessedRTCM
     * @return whether the RTCM type set contains any MSM typs.
     */
    public static boolean hasMSMTypes(ProcessedRTCM rtcm) {
        if (rtcm == null) return false;
        var props = rtcm.getProperties();
        if (props == null) return false;
        Set<Integer> messageTypes = props.getMessageTypes();
        if (messageTypes == null) return false;
        return messageTypes.stream().anyMatch(messageType
                -> (messageType != null && messageType >= 1070 && messageType <= 1129));
    }

    /**
     * Comparison methods for RTCM Message Count Progression Events.
     * Compare everything other than the message count, timestamps, and metadata that isn't part of the
     * original ASN.1 message.
     * @param rtcmA The first RTCM
     * @param rtcmB A following RTCM
     * @return DiffResult with differences for each field
     */
    public static DiffResult<ProcessedRTCM> compare(ProcessedRTCM rtcmA, ProcessedRTCM rtcmB) {
        var builder = new ReflectionDiffBuilder<ProcessedRTCM>(rtcmA, rtcmB, ToStringStyle.SHORT_PREFIX_STYLE)
                .setExcludeFieldNames("odeReceivedAt", "utcTime", "schemaVersion", "messageType", "originIp", "asn1",
                        "validationMessages", "cti4501Conformant");
        var result = builder.build();
        log.debug("RTCM Diff result: {}", result);
        return result;
    }
}
