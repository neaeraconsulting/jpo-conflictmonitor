package us.dot.its.jpo.conflictmonitor.monitor.utils;

import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

import java.util.Set;

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
}
