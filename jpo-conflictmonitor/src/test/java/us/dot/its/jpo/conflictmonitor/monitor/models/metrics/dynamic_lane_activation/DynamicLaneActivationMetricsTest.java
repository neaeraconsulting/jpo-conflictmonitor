package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartMatches;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static us.dot.its.jpo.conflictmonitor.testutils.DynamicLaneActivationMetricsTestUtils.createMetrics;

/**
 * Test serializing/deeserializing the data structure
 */
@Slf4j
public class DynamicLaneActivationMetricsTest {

    private static final ObjectMapper mapper = DateJsonMapper.getInstance();

    final String source = "127.0.0.1";
    final int intersectionID = 10020;
    final int roadRegulatorID = 22100;
    final OffsetDateTime startTime = OffsetDateTime.of(2026, 1, 2, 16, 0, 0, 0, ZoneOffset.UTC);
    final OffsetDateTime endTime = startTime.plusHours(1);
    final Duration interval = Duration.ofMinutes(10);

    @Test
    public void testSerializeDynamicLaneActivationMetrics() throws JsonProcessingException {
        DynamicLaneActivationMetrics metrics = createMetrics(source, intersectionID, roadRegulatorID, startTime, endTime, interval);
        String serialized = mapper.writeValueAsString(metrics);
        log.info(serialized);
        assertThat(serialized, notNullValue());
        assertThat(serialized, jsonPartEquals("source", source));
        assertThat(serialized, jsonPartEquals("intersectionID", intersectionID));
        assertThat(serialized, jsonPartEquals("roadRegulatorID", roadRegulatorID));
        assertThat(serialized, jsonPartMatches("revocableEnabledLaneStatusTable", hasSize(4)));
    }
}
