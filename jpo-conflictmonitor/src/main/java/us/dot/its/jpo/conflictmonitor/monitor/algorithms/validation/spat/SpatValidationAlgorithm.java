package us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.Algorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.spat.SpatMinimumDataAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event_state_progression.EventStateProgressionAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression.SpatMessageCountProgressionAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.spat.SpatTimeChangeDetailsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaAlgorithm;

/**
 * Interface for SPaT validation algorithms.
 * <p>
 * Extends the base {@link Algorithm} interface for SPaT validation parameters.
 * Also hosts Spat Time Change Details, Spat Message Count Progression, and
 * Event State Progression as plugins so ProcessedSpat is consumed once.
 */
public interface SpatValidationAlgorithm
    extends Algorithm<SpatValidationParameters> {

    /**
     * Gets the SPaT timestamp delta algorithm used for validation.
     *
     * @return the SpatTimestampDeltaAlgorithm instance
     */
    SpatTimestampDeltaAlgorithm getTimestampDeltaAlgorithm();

    /**
     * Sets the SPaT timestamp delta algorithm used for validation.
     *
     * @param timestampDeltaAlgorithm the SpatTimestampDeltaAlgorithm to set
     */
    void setTimestampDeltaAlgorithm(SpatTimestampDeltaAlgorithm timestampDeltaAlgorithm);

    /**
     * Sets the minimum data aggregation algorithm used for SPaT validation.
     *
     * @param spatMinimumDataAggregationAlgorithm the SpatMinimumDataAggregationAlgorithm to set
     */
    void setMinimumDataAggregationAlgorithm(SpatMinimumDataAggregationAlgorithm spatMinimumDataAggregationAlgorithm);

    /**
     * Sets the Spat Time Change Details plugin (event-time branch).
     */
    void setSpatTimeChangeDetailsAlgorithm(SpatTimeChangeDetailsAlgorithm spatTimeChangeDetailsAlgorithm);

    /**
     * Sets the Spat Message Count Progression plugin (event-time branch).
     */
    void setSpatMessageCountProgressionAlgorithm(SpatMessageCountProgressionAlgorithm spatMessageCountProgressionAlgorithm);

    /**
     * Sets the Event State Progression plugin (event-time branch). Optional when disabled.
     */
    void setEventStateProgressionAlgorithm(EventStateProgressionAlgorithm eventStateProgressionAlgorithm);
}
