package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Test serializing/deeserializing the data structure
 */
public class DynamicLaneActivationMetricsTest {

    private static final ObjectMapper mapper = DateJsonMapper.getInstance();

    final String source = "127.0.0.1";
    final int intersectionID = 10020;
    final int roadRegulatorID = 22100;
    final OffsetDateTime startTime = OffsetDateTime.of(2026, 1, 2, 9, 0, 0, 0, ZoneOffset.UTC);
    final OffsetDateTime endTime = startTime.plusHours(1);

    @Test
    public void testSerializeDynamicLaneActivationMetrics() {
        var metrics = new DynamicLaneActivationMetrics();
        metrics.setSource(source);
        metrics.setIntersectionID(intersectionID);
        metrics.setRoadRegulatorID(roadRegulatorID);
        metrics.setTimePeriod(new ProcessingTimePeriod(startTime.toInstant().toEpochMilli(), endTime.toInstant().toEpochMilli()));
        metrics.setKey(new RsuIntersectionKey(source, intersectionID, roadRegulatorID));
        var statusTable = new RevocableEnabledLaneStatusTable();
        statusTable.put(16, new RevocableEnabledLaneStatusChanges());
        statusTable.put(17, new RevocableEnabledLaneStatusChanges());
        statusTable.put(80, new RevocableEnabledLaneStatusChanges());
        statusTable.put(81, new RevocableEnabledLaneStatusChanges());
        metrics.setRevocableEnabledLaneStatusMap(statusTable);
    }
}
