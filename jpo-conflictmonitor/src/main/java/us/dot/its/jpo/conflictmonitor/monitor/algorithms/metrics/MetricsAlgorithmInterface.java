package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.ConfigurableAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.Metrics;

public interface MetricsAlgorithmInterface<TMetric extends Metrics<TKey>, TKey>
    extends ConfigurableAlgorithm<MetricsParameters> {
}
