package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/** Shared aggregation for topology gauges and the streams CPU health endpoint. */
public record StreamsThreadMetrics(double processRatio, double processLatencyAvgMs,
        double processLatencyMaxMs, double pollRatio, double processRate, int threadCount) {

    public static StreamsThreadMetrics from(Map<MetricName, ? extends Metric> metrics) {
        double ratioSum = 0;
        int ratioCount = 0;
        double latencySum = 0;
        int latencyCount = 0;
        double latencyMax = 0;
        double pollSum = 0;
        int pollCount = 0;
        double rateSum = 0;

        for (var entry : metrics.entrySet()) {
            MetricName name = entry.getKey();
            if (!"stream-thread-metrics".equals(name.group())) {
                continue;
            }
            Object value = entry.getValue().metricValue();
            if (!(value instanceof Number number)) {
                continue;
            }
            double numeric = number.doubleValue();
            switch (name.name()) {
                case "process-ratio" -> {
                    ratioSum += numeric;
                    ratioCount++;
                }
                case "process-latency-avg" -> {
                    latencySum += numeric;
                    latencyCount++;
                }
                case "process-latency-max" -> latencyMax = Math.max(latencyMax, numeric);
                case "poll-ratio" -> {
                    pollSum += numeric;
                    pollCount++;
                }
                case "process-rate" -> rateSum += numeric;
                default -> { /* Other thread metrics do not contribute to compute summaries. */ }
            }
        }
        return new StreamsThreadMetrics(ratioCount > 0 ? ratioSum / ratioCount : 0,
                latencyCount > 0 ? latencySum / latencyCount : 0, latencyMax,
                pollCount > 0 ? pollSum / pollCount : 0, rateSum, ratioCount);
    }
}
