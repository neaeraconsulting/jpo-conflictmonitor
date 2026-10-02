package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_message_count_progression.MapMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.MapMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.ProcessedMapDeserializer;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
public class MapMessageCountProgressionTopologyTest {

    private static final String mapInputTopicName = "topic.ProcessedMap";
    private static final String eventOutputTopicName = "topic.CmMapMessageCountProgressionEvents";
    private static final String processedMapStateStoreName = "processedMapStateStore";
    private static final String latestMapStateStoreName = "latestMapStateStore";
    private static final int bufferTimeMs = 5000;
    private static final int bufferGracePeriodMs = 800;
    private static final boolean debug = true;
    private static final boolean aggregateEvents = false;

    final String rsuId = "127.18.0.1";
    final int intersectionId = 12112;
    final int region = -1;

    @Test
    public void testSameContentSameRevisionProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        testTopology(map1, map2, false);
    }

    @Test
    public void testSameContentSameRevisionDifferentMetadataProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        var props2 = map2.getProperties();
        var ts = props2.getTimeStamp();
        props2.setTimeStamp(ts.plusMinutes(1));
        props2.setOriginIp("10.10.10.10");
        props2.setAsn1("00123456789a");
        props2.setValidationMessages(List.of(new ProcessedValidationMessage()));
        props2.setMapSource(OdeMessageFrameMetadata.Source.NA);
        testTopology(map1, map2, false);
    }

    @Test
    public void testSameContentDifferentRevisionProducesEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        var props2 = map2.getProperties();
        final var revision = props2.getRevision();
        final var msgRevision = props2.getMsgIssueRevision();
        props2.setRevision(revision + 1);
        props2.setMsgIssueRevision(msgRevision + 1);
        testTopology(map1, map2, true);
    }

    @Test
    public void testDifferentContentSameRevisionProducesEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        changeCoordinate(map2);
        testTopology(map1, map2, true);
    }

    @Test
    public void testDifferentContentRevisionIncrementedByOneProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        changeCoordinate(map2);
        var props2 = map2.getProperties();
        props2.setRevision(props2.getRevision() + 1);
        testTopology(map1, map2, false);
    }

    @Test
    public void testDifferentContentMsgIssueRevisionIncrementedByOneProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        changeCoordinate(map2);
        var props2 = map2.getProperties();
        props2.setMsgIssueRevision(props2.getMsgIssueRevision() + 1);
        testTopology(map1, map2, false);
    }

    @Test
    public void testDifferentContentBothRevisionsIncrementedByOneProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        changeCoordinate(map2);
        var props2 = map2.getProperties();
        props2.setRevision(props2.getRevision() + 1);
        props2.setMsgIssueRevision(props2.getMsgIssueRevision() + 1);
        testTopology(map1, map2, false);
    }

    @Test
    public void testDifferentContentRevisionIncrementedByTwoProducesEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        changeCoordinate(map2);
        var props2 = map2.getProperties();
        props2.setRevision(props2.getRevision() + 2);
        testTopology(map1, map2, true);
    }

    @Test
    public void testDifferentContentRevisionIncrementedByOneWithRolloverProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        changeCoordinate(map2);
        map1.getProperties().setRevision(127);
        map2.getProperties().setRevision(0);
        testTopology(map1, map2, false);
    }

    // max timestamp offset 1500ms
    @Test
    public void testSameContentDifferentRevisionTimestampOffsetGreaterThanMaxProducesNoEvent() throws IOException {
        var map1 = loadSampleMap();
        var map2 = loadSampleMap();
        var props2 = map2.getProperties();
        props2.setRevision(props2.getRevision() + 1);
        testTopology(map1, map2, false, 2000L);
    }

    private void changeCoordinate(ProcessedMap<LineString> map) {
        final double[] coord = map.getMapFeatureCollection().getFeatures()[0].getGeometry().getCoordinates()[0];
        final double[] newCoord = new double[]{coord[0] + 0.001, coord[1] + 0.001};
        map.getMapFeatureCollection().getFeatures()[0].getGeometry().getCoordinates()[0] = newCoord;
    }

    private void testTopology(ProcessedMap<LineString> map1, ProcessedMap<LineString> map2, boolean expectEvent) {
        testTopology(map1, map2, expectEvent, 1000L);
    }

    private void testTopology(ProcessedMap<LineString> map1, ProcessedMap<LineString> map2, boolean expectEvent, long map2OffsetMs) {
        Topology topology = createTopology();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology);
             Serde<RsuIntersectionKey> keySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedMap<LineString>> processedMapSerdes
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson();
             Serde<MapMessageCountProgressionEvent> eventSerde = JsonSerdes.MapMessageCountProgressionEvent();
        ) {
            var inputTopic = driver.createInputTopic(mapInputTopicName,
                    keySerde.serializer(),
                    processedMapSerdes.serializer());

            var outputTopic = driver.createOutputTopic(eventOutputTopicName,
                    keySerde.deserializer(),
                    eventSerde.deserializer());

            final Instant startTime = Instant.ofEpochMilli(1674356320000L);
            final Instant map1Time = startTime.plusMillis(1000L);
            final Instant map2Time = map1Time.plusMillis(map2OffsetMs);
            final Instant map8Time = startTime.plusMillis(8000L);


            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);

            // Baseline map
            map1.getProperties().setOdeReceivedAt(ZonedDateTime.ofInstant(map1Time, ZoneOffset.UTC));
            inputTopic.pipeInput(key, map1, map1Time);

            // Possibly changed map
            map2.getProperties().setOdeReceivedAt(ZonedDateTime.ofInstant(map2Time, ZoneOffset.UTC));
            inputTopic.pipeInput(key, map2, map2Time);

            // Send changed map again to advance stream time beyond the buffer + grace period to get event to be produced
            map2.getProperties().setOdeReceivedAt(ZonedDateTime.ofInstant(map8Time, ZoneOffset.UTC));
            inputTopic.pipeInput(key, map2, map8Time);


            var events = outputTopic.readKeyValuesToList();
            if (expectEvent) {
                assertThat("expected event", events, hasSize(1));
            } else {
                assertThat("unexpected event", events, hasSize(0));
            }
        }
    }


    private Topology createTopology() {
        var parameters = getParameters();
        var mapValidationTopology = new MapMessageCountProgressionTopology();
        mapValidationTopology.setParameters(parameters);
        return mapValidationTopology.buildTopology();
    }



    private MapMessageCountProgressionParameters getParameters() {
        var parameters = new MapMessageCountProgressionParameters();
        parameters.setDebug(debug);
        parameters.setMapInputTopicName(mapInputTopicName);
        parameters.setMapMessageCountProgressionEventOutputTopicName(eventOutputTopicName);
        parameters.setProcessedMapStateStoreName(processedMapStateStoreName);
        parameters.setLatestMapStateStoreName(latestMapStateStoreName);
        parameters.setBufferTimeMs(bufferTimeMs);
        parameters.setBufferGracePeriodMs(bufferGracePeriodMs);
        parameters.setAggregateEvents(aggregateEvents);
        return parameters;
    }

    private static final String SAMPLE_MAP_RESOURCE = "/us/dot/its/jpo/conflictmonitor/monitor/topologies/sample.processed-map.json";

    private ProcessedMap<LineString> loadSampleMap() throws IOException {
        try (InputStream in = getClass().getResourceAsStream(SAMPLE_MAP_RESOURCE);
             var deserializer = new ProcessedMapDeserializer<>(LineString.class)) {
            assertThat(in, notNullValue());
            byte[] bytes = IOUtils.toByteArray(in);
            return deserializer.deserialize("test-topic", bytes);
        }
    }
}
