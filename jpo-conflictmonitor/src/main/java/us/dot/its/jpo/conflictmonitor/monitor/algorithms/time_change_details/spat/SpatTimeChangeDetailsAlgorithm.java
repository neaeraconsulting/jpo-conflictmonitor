package us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.spat;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.ConfigurableAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.time_change_details.TimeChangeDetailsAggregationAlgorithm;

/**
 * Spat Time Change Details algorithm; plugs into SpatValidationTopology
 * (single consume of ProcessedSpat).
 */
public interface SpatTimeChangeDetailsAlgorithm
        extends ConfigurableAlgorithm<SpatTimeChangeDetailsParameters> {

    void setAggregationAlgorithm(TimeChangeDetailsAggregationAlgorithm aggregationAlgorithm);

}
