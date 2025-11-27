package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Message Count Progression Event for RTCM messages as defined in Table 43 of the SDD (July 2025 rev)
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
@JsonIgnoreProperties(ignoreUnknown = true)
public class RtcmMessageCountProgressionEvent extends Event {
    public RtcmMessageCountProgressionEvent() {
        super("RtcmMessageCountProgression");
    }

    /**
     *  String representing the source of the RTCM message. Typically the RSU IP address
     */
    private String source;

    /**
     * The message type, always "RTCM"
     */
    private final String messageType = "RTCM";

    /**
     * The SDD states this is "The address of the data frame where the message count for which the comparison is being
     * performed is located" which is not applicable to the textual representation of the messages used here, so
     * we use the name of the data frame containing the message count, which for RTCMcorrections types is always
     * "MSG_RTCMcorrections"
     */
    private final String dataFrame = "MSG_RTCMcorrections";

    /**
     * The SDD states this is "The address of the first data frame or data element where a change in value was observed.
     * If all data remains the same, this value is null."
     * Using the ProcessedRTSM JSON objects consumed by this application, we instead report a list of the names and
     * indices of all data elements that are changed, or an empty list if there were no changes.
     */
    private List<String> change = new ArrayList<String>();

    /**
     * The value of the message count data element in the first message
     */
    private int messageCountA;

    /**
     * The value of the message count data element in the subsequent message
     */
    private int messageCountB;

    /**
     * The timestamp from the first RTCM message. Timestamp is formatted in UTC milliseconds.
     */
    private long timestampA;

    /**
     * The timestamp from the subsequent RTCM message. Timestamp is formatted in UTC milliseconds.
     */
    private long timestampB;
}
