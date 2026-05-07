package us.dot.its.jpo.conflictmonitor.batch.events;

import java.time.ZonedDateTime;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "eventType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = AtspmSpatPairEvent.class, name = "AtspmSpatPair"),
})
@Data
@Slf4j
public abstract class Event {
    /**
     * long representing the utc timestamp in milliseconds when this event was generated. This value is automatically created by the event when the object is generated. It doesn't represent the time that the actual data occurred.
     * It is recommended to use this value for indexing and data retrieval as it is common among all events. In general this timestamp is within 1 - 2 seconds of the actual time at which an event occurred depending on the event.
     */
    private final long eventGeneratedAt = ZonedDateTime.now().toInstant().toEpochMilli();

    /**
     * A string representing the time of event this class represents. This is used for automatically decoding and parsing event types with Jackson.
     */
    private String eventType;


    /**
     * int representing the intersectionID where this event occurred. If this event didn't take place at an intersection (such as with vehicle Misbehavior events) a value of -1 is used instead.
     */
    private int intersectionID = -1;

    /**
     * int representing the roadRegulatorID of the intersection where this event occurred. Generally set to -1, roadRegulator is in the process of being deprecated and shouldn't be used.
     */
    private int roadRegulatorID = -1;


    public Event(String eventType) {
        this.eventType = eventType;
    }


    @Override
    public String toString() {
        try {
            return DateJsonMapper.getInstance().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error(String.format("Exception serializing %s Event to JSON", eventType), e);
        }
        return "";
    }
}
