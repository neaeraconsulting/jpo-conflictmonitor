package us.dot.its.jpo.conflictmonitor.testutils;

import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.*;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class DynamicLaneActivationMetricsTestUtils {



    public static DynamicLaneActivationMetrics createMetrics(final String source, final int intersectionID,
                       final int roadRegulatorID, final OffsetDateTime startTime, final OffsetDateTime endTime,
                       final Duration interval) {
        var metrics = new DynamicLaneActivationMetrics();
        var key = new RsuIntersectionKey(source, intersectionID, roadRegulatorID);
        metrics.setKey(key);
        var timePeriod = new ProcessingTimePeriod(startTime.toInstant().toEpochMilli(), endTime.toInstant().toEpochMilli());
        metrics.setTimePeriod(timePeriod);
        var table = new RevocableEnabledLaneStatusTable();
        table.add(createChanges(16, startTime, endTime, interval, true));
        table.add(createChanges(17, startTime, endTime, interval, false));
        table.add(createChanges(80, startTime, endTime, interval, true));
        table.add(createChanges(81, startTime, endTime, interval, false));
        metrics.setRevocableEnabledLaneStatusTable(table);
        return metrics;
    }

    public static RevocableEnabledLaneStatusChanges createChanges(int laneId, final OffsetDateTime startTime,
                                                                  final OffsetDateTime endTime,
                                                                  final Duration interval,
                                                                  final boolean firstValue) {
        var changes = new RevocableEnabledLaneStatusChanges();
        changes.setLaneID(laneId);
        var statusList = new RevocableEnabledStatusList();
        OffsetDateTime currentTime = startTime;
        boolean currentValue = firstValue;
        while (currentTime.isBefore(endTime)) {
            long currentTimestamp = currentTime.toInstant().toEpochMilli();
            statusList.add(new RevocableEnabledStatus(currentTimestamp, currentValue));
            currentTime = currentTime.plus(interval);
            currentValue = !currentValue;
        }
        changes.setStatusChanges(statusList);
        return changes;
    }
}
