package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Getter;

/**
 * Includes a subset of logger Event Codes used by this algorithm to indication the beginning of green, yellow and red
 * indications.
 * <p>Note: There is no enumeration in the C# code for the logger Event Codes enumeration. For the full set of those,
 *  see:
 *  <p><a href="https://doi.org/10.5703/1288284316998">
 *      Indiana Traffic Signal Hi Resolution Data Logger Enumerations, 2019 revision.
 *  </a>
 */
@Getter
public enum EventCode {
    GREEN(1, "Phase Begin Green"),
    YELLOW(8, "Phase Begin Yellow Change"),
    RED(10, "Phase Begin Red Clearance");

    private final int code;
    private final String descriptor;

    EventCode(int code, String descriptor) {
        this.code = code;
        this.descriptor = descriptor;
    }

    public static EventCode fromCode(int code) {
        for (EventCode eventCode : EventCode.values()) {
            if (eventCode.code == code) {
                return eventCode;
            }
        }
        throw new IllegalArgumentException("Invalid EventCode code: " + code);
    }

}
