package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.ConfigurableAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.Metrics;

public interface MetricsAlgorithmInterface<TMetric extends Metrics<TKey>, TKey, TParams>
    extends ConfigurableAlgorithm<TParams>{

    /**
     * @param parameters Common parameters for aggregation
     */
    void setCommonParameters(CommonMetricsParameters parameters);
    CommonMetricsParameters getCommonParameters();


}
