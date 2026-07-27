package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import lombok.*;

/**
 * A processing time period with begin and end timestamps
 */
@Getter
@Setter
@EqualsAndHashCode
@Generated
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingTimePeriod {
    
    /**
     * The timestamp at the beginning of the processing period in epoch milliseconds
     */
    private long beginTimestamp;

    /**
     * The timestamp at the end of the processing period in epoch milliseconds
     */
    private long endTimestamp;

    
  
    public long periodMillis() {
        return Math.abs(endTimestamp - beginTimestamp); 
    }

}
