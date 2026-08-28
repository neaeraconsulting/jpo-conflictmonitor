package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.MapSharedProperties;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.RTCMProperties;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class TimestampExtractorForBroadcastRateTest {

    private static final String TOPIC = "topic.Test";

    @Test
    public void testExtractTimestamp_Spat_Primary() {
        var spat = new ProcessedSpat();
        var zdt = ZonedDateTime.now(ZoneOffset.UTC);
        spat.setUtcTimeStamp(zdt);
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(spat);
        assertThat(result, equalTo(zdt.toInstant().toEpochMilli()));
    }

    @Test
    public void testExtractTimestamp_Spat_Fallback() {
        var spat = new ProcessedSpat();
        var zdt = ZonedDateTime.now(ZoneOffset.UTC);
        spat.setOdeReceivedAt(zdt.format(DateTimeFormatter.ISO_DATE_TIME));
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(spat);
        assertThat(result, equalTo(zdt.toInstant().toEpochMilli()));
    }

    @Test
    public void testExtractTimestamp_Spat_Failure() {
        var spat = new ProcessedSpat();
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(spat);
        assertThat(result, equalTo(-1L));
    }

    @Test
    public void testExtractTimestamp_Map_Success() {
        var map = new ProcessedMap<>();
        var props = new MapSharedProperties();
        var zdt = ZonedDateTime.now(ZoneOffset.UTC);
        props.setOdeReceivedAt(zdt);
        map.setProperties(props);
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(map);
        assertThat(result, equalTo(zdt.toInstant().toEpochMilli()));
    }

    @Test
    public void testExtractTimestamp_Map_Failure() {
        var map = new ProcessedMap<>();
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(map);
        assertThat(result, equalTo(-1L));
    }

    @Test
    public void testExtractTimestamp_Rtcm_Primary() {
        var props = new RTCMProperties();
        props.setUtcTime(1234567890000L);
        var rtcm = new ProcessedRTCM(new Point(-105.0, 40.0), props);
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(rtcm);
        assertThat(result, equalTo(1234567890000L));
    }

    @Test
    public void testExtractTimestamp_Rtcm_Fallback() {
        var props = new RTCMProperties();
        var zdt = ZonedDateTime.now(ZoneOffset.UTC);
        props.setOdeReceivedAt(zdt);
        var rtcm = new ProcessedRTCM(new Point(-105.0, 40.0), props);
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(rtcm);
        assertThat(result, equalTo(zdt.toInstant().toEpochMilli()));
    }

    @Test
    public void testExtractTimestamp_Rtcm_Failure() {
        var props = new RTCMProperties();
        var rtcm = new ProcessedRTCM(new Point(-105.0, 40.0), props);
        long result = TimestampExtractorForBroadcastRate.extractTimestamp(rtcm);
        assertThat(result, equalTo(-1L));
    }

    @Test
    public void testExtract_Spat() {
        var spat = new ProcessedSpat();
        var zdt = ZonedDateTime.now(ZoneOffset.UTC);
        spat.setUtcTimeStamp(zdt);
        var record = new ConsumerRecord<Object, Object>(TOPIC, 0, 0L, null, spat);
        var extractor = new TimestampExtractorForBroadcastRate();
        long result = extractor.extract(record, 999L);
        assertThat(result, equalTo(zdt.toInstant().toEpochMilli()));
    }

    @Test
    public void testExtract_Map() {
        var map = new ProcessedMap<>();
        var props = new MapSharedProperties();
        var zdt = ZonedDateTime.now(ZoneOffset.UTC);
        props.setOdeReceivedAt(zdt);
        map.setProperties(props);
        var record = new ConsumerRecord<Object, Object>(TOPIC, 0, 0L, null, map);
        var extractor = new TimestampExtractorForBroadcastRate();
        long result = extractor.extract(record, 999L);
        assertThat(result, equalTo(zdt.toInstant().toEpochMilli()));
    }

    @Test
    public void testExtract_Rtcm() {
        var props = new RTCMProperties();
        props.setUtcTime(1234567890000L);
        var rtcm = new ProcessedRTCM(new Point(-105.0, 40.0), props);
        var record = new ConsumerRecord<Object, Object>(TOPIC, 0, 0L, null, rtcm);
        var extractor = new TimestampExtractorForBroadcastRate();
        long result = extractor.extract(record, 999L);
        assertThat(result, equalTo(1234567890000L));
    }

    @Test
    public void testExtract_FailureFallsBackToPartitionTime() {
        var spat = new ProcessedSpat();
        var record = new ConsumerRecord<Object, Object>(TOPIC, 0, 0L, null, spat);
        var extractor = new TimestampExtractorForBroadcastRate();
        long result = extractor.extract(record, 42L);
        assertThat(result, equalTo(42L));
    }

    @Test
    public void testExtract_FailureAndInvalidPartitionTimeFallsBackToClock() {
        var spat = new ProcessedSpat();
        var record = new ConsumerRecord<Object, Object>(TOPIC, 0, 0L, null, spat);
        var extractor = new TimestampExtractorForBroadcastRate();
        long before = Instant.now().toEpochMilli();
        long result = extractor.extract(record, -1L);
        long after = Instant.now().toEpochMilli();
        assertThat(result, greaterThanOrEqualTo(before));
        assertThat(result, lessThanOrEqualTo(after));
    }

    @Test
    public void testExtract_UnrecognizedTypeFallsBackToPartitionTime() {
        var record = new ConsumerRecord<Object, Object>(TOPIC, 0, 0L, null, "not a recognized type");
        var extractor = new TimestampExtractorForBroadcastRate();
        long result = extractor.extract(record, 42L);
        assertThat(result, equalTo(42L));
    }
}
