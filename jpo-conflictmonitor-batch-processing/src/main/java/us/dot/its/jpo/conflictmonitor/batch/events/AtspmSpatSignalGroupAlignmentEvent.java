package us.dot.its.jpo.conflictmonitor.batch.events;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("CmAtspmSpatSignalGroupAlignmentEvent")
public class AtspmSpatSignalGroupAlignmentEvent extends Event {

    public AtspmSpatSignalGroupAlignmentEvent() {
        super("AtspmSpatSignalGroupAlignment");
    }

    private String signalId;
    private String intersectionDescription;
    private Instant startTime;
    private Instant endTime;

    private Set<Integer> spatSignalGroupIds = new TreeSet<>();
    private Set<Integer> mappedPhasesFromSpats = new TreeSet<>();
    private Set<Integer> atspmPhases = new TreeSet<>();
}
