package us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.ConfigurableAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.spat_message_count_progression.SpatMessageCountProgressionAggregationAlgorithm;

/**
 * SPaT message count progression algorithm; plugs into SpatValidationTopology
 * (single consume of ProcessedSpat).
 */
public interface SpatMessageCountProgressionAlgorithm
        extends ConfigurableAlgorithm<SpatMessageCountProgressionParameters> {

    /**
     * Sets the AggregationAlgorithm used for validation.
     *
     * @param aggregationAlgorithm the aggregationAlgorithm to set
     */
    void setAggregationAlgorithm(SpatMessageCountProgressionAggregationAlgorithm aggregationAlgorithm);

}
