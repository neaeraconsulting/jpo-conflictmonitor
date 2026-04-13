package us.dot.its.jpo.conflictmonitor.monitor.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.*;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.DecodedRTCMmessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.RTCMProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class RtcmUtils {

    /**
     * Checks for Multiple Signal Messages (MSM) RTCM types, which are 1070-1129
     * Ref. <a href="https://www.use-snip.com/kb/knowledge-base/an-rtcm-message-cheat-sheet/">an-rtcm-message-cheat-sheet</a>
     *
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
     * Compare everything other than the message count, timestamps, and metadata added by the ODE and GeoJSON converter
     * that isn't part of the original ASN.1 message.
     * @param rtcmA The first RTCM
     * @param rtcmB A following RTCM
     * @return DiffResult with differences for each field, or an empty list if no differences.
     */
    public static DiffResult<ProcessedRTCM> compare(ProcessedRTCM rtcmA, ProcessedRTCM rtcmB) {

        if (rtcmA == null || rtcmB == null) {
            throw new IllegalArgumentException("Either rtcmA or rtcmB is null, nothing to compare");
        }

        var rtcmDiffBuilder = new DiffBuilder<ProcessedRTCM>(rtcmA, rtcmB, ToStringStyle.JSON_STYLE);

        final Point geomA = rtcmA.getGeometry();
        final Point geomB = rtcmB.getGeometry();
        if (geomA != null && geomB != null) {
            var geomDiffBuilder = new ReflectionDiffBuilder<Point>(rtcmA.getGeometry(), rtcmB.getGeometry(),
                    ToStringStyle.JSON_STYLE);
            DiffResult<Point> geomDiffResult = geomDiffBuilder.build();
            for (var geomDiff : geomDiffResult.getDiffs()) {
                rtcmDiffBuilder.append(geomDiff.getFieldName(), geomDiff.getLeft(), geomDiff.getRight());
            }
        } else {
            // One is missing geometry, note that difference
            rtcmDiffBuilder.append("geometry", geomA != null, geomB != null);
        }

        final RTCMProperties propertiesA = rtcmA.getProperties();
        final RTCMProperties propertiesB = rtcmB.getProperties();
        if (propertiesA != null && propertiesB != null) {
            var propertiesDiffBuilder = new ReflectionDiffBuilder<RTCMProperties>(rtcmA.getProperties(),
                                                                    rtcmB.getProperties(), ToStringStyle.JSON_STYLE)
                    .setExcludeFieldNames(
                            // Ignore timestamps
                            "odeReceivedAt",
                            "utcTime",
                            // Ignore msgCnt
                            "msgCnt",
                            // Will check message contents separately
                            "messages",
                            // Ignore metadata added by the ODE and GJC
                            "schemaVersion",
                            "messageType",
                            "originIp",
                            "asn1",
                            "validationMessages",
                            "cti4501Conformant");
            DiffResult<RTCMProperties> propertiesDiffResult = propertiesDiffBuilder.build();
            for (var propertiesDiff : propertiesDiffResult.getDiffs()) {
                rtcmDiffBuilder.append(propertiesDiff.getFieldName(), propertiesDiff.getLeft(),
                        propertiesDiff.getRight());
            }
        } else {
            // Note properties entirely missing
            rtcmDiffBuilder.append("properties", propertiesA != null, propertiesB != null);
            return rtcmDiffBuilder.build();
        }

        // Message details differences
        final List<DecodedRTCMmessage> messagesA = propertiesA.getMessages();
        final List<DecodedRTCMmessage> messagesB = propertiesB.getMessages();
        if (messagesA != null && messagesB != null) {
            final int messageCountA = messagesA.size();
            final int messageCountB = messagesB.size();
            if (messageCountA != messageCountB) {
                rtcmDiffBuilder.append("message-count", messageCountA, messageCountB);
            } else {
                for (int i = 0; i < messageCountA; ++i) {
                    final DecodedRTCMmessage messageA = messagesA.get(i);
                    final DecodedRTCMmessage messageB = messagesB.get(i);
                    rtcmDiffBuilder.append("message-" + i + "-hex", messageA.getHex(), messageB.getHex());
                    final JsonNode decodedA = messageA.getDecodedMessage();
                    final JsonNode decodedB = messageB.getDecodedMessage();
                    if (messageA.getDecodedMessage() instanceof ObjectNode objA
                            && messageB.getDecodedMessage() instanceof ObjectNode objB) {
                        var fieldNameIterator = objA.fieldNames();
                        while (fieldNameIterator.hasNext()) {
                            final var fieldName = fieldNameIterator.next();
                            final JsonNode aValue = objA.get(fieldName);
                            final JsonNode bValue = objB.get(fieldName);
                            rtcmDiffBuilder.append("message-" + i + "-" + fieldName, aValue, bValue);
                        }
                    }
                }
            }
        } else {
            rtcmDiffBuilder.append("messages", messagesA != null, messagesB != null);
        }

        return rtcmDiffBuilder.build();
    }

    public static List<String> listDifferingFields(DiffResult<ProcessedRTCM> diffResult) {
        return diffResult.getDiffs().stream().map(Diff::getFieldName).toList();
    }
}
