package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuVehicleIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedPriorityRequestType;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSrm;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.SrmProperties;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSsm;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestConstants.DEFAULT_PRIORITY_PREEMPTION_REQUEST_ALGORITHM;

public class PriorityPreemptionRequestTopologyTest {

    private static final String inputSrmTopicName = "topic.ProcessedSrm";
    private static final String inputSsmTopicName = "topic.ProcessedSsm";
    private static final String outputEventTopicName = "topic.CmPriorityPreemptionRequestEvent";
    private static final String rsuId = "127.0.0.1";
    private static final String vehicleId = "ABCD0102";
    private static final double lon = -104.0;
    private static final double lat = 40.0;

    private static final int intersectionId = 12115;
    private static final int roadRegulatorId = 22100;
    private static final int requestId = 10;
    private static final int inboundLaneId = 15;
    private static final int outboundLaneId = 22;
    private static final long secondsLatency = 2L;
    private static final ProcessedBasicVehicleRole vehicleType = ProcessedBasicVehicleRole.PUBLICTRANSPORT;
    private static final ProcessedPriorityRequestType requestType = ProcessedPriorityRequestType.PRIORITYREQUEST;


    /**
     * Test that a matched SRM and SSM emit an event
     */
    @Test
    public void testPriorityPreemptionRequestEvent() {
        Topology topology = createTopology();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            var inputSrmTopic = driver.createInputTopic(inputSrmTopicName,
                    new JsonSerializer<RsuVehicleIdKey>(),
                    new JsonSerializer<ProcessedSrm>());

            var inputSsmTopic = driver.createInputTopic(inputSsmTopicName,
                    new JsonSerializer<RsuIntersectionKey>(),
                    new JsonSerializer<ProcessedSsm>());

            var outputEventTopic = driver.createOutputTopic(outputEventTopicName,
                    new JsonDeserializer<>(IntersectionVehicleRequestKey.class),
                    new JsonDeserializer<>(PriorityPreemptionRequestEvent.class));

            final RsuVehicleIdKey rsuVehicleIdKey = new RsuVehicleIdKey(rsuId, vehicleId);
            final RsuIntersectionKey rsuIntersectionKey = new RsuIntersectionKey(rsuId, intersectionId, roadRegulatorId);
            final ZonedDateTime now = ZonedDateTime.of(2025, 9, 30, 9, 46,
                    55, 0, ZoneOffset.UTC);
            final ZonedDateTime nowPlus1 = now.plusSeconds(1);
            final ProcessedSrm processedSrm = createSrm(now);
            final ProcessedSsm processedSsm = createSsm(nowPlus1, ProcessedPrioritizationResponseStatus.GRANTED);

            inputSrmTopic.pipeInput(rsuVehicleIdKey, processedSrm, now.toInstant());
            inputSsmTopic.pipeInput(rsuIntersectionKey, processedSsm, nowPlus1.toInstant());

            var eventList = outputEventTopic.readKeyValuesToList();
            assertThat(eventList, hasSize(1));
            var keyEvent = eventList.getFirst();
            var resultKey = keyEvent.key;
            assertThat(resultKey, notNullValue());
            assertThat(resultKey.getRequestId(), equalTo(requestId));
            assertThat(resultKey.getIntersectionId(), equalTo(intersectionId));
            assertThat(resultKey.getRegion(), equalTo(roadRegulatorId));
            assertThat(resultKey.getVehicleId(), equalTo(vehicleId));
            var resultValue = keyEvent.value;
            assertThat(resultValue, notNullValue());
            assertThat(resultValue.getRequestId(), equalTo(requestId));
            assertThat(resultValue.getPriorityRequestType(), equalTo(requestType));
            assertThat(resultValue.getFinalStatus(), equalTo(ProcessedPrioritizationResponseStatus.GRANTED));
            // TODO...
        }
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var topology = new PriorityPreemptionRequestTopology();
        topology.setParameters(parameters);
        return topology.buildTopology();
    }

    private PriorityPreemptionRequestParameters getParameters() {
        var parameters = new PriorityPreemptionRequestParameters();
        parameters.setAlgorithm(DEFAULT_PRIORITY_PREEMPTION_REQUEST_ALGORITHM);
        parameters.setDebug(true);
        parameters.setOutputEventTopic(outputEventTopicName);
        parameters.setSrmStoreName("srm-store");
        parameters.setSrmStoreRetentionTimeMinutes(10);
        parameters.setSsmStreamGracePeriodMilliseconds(5000);
        parameters.setProcessedSrmInputTopic(inputSrmTopicName);
        parameters.setProcessedSsmInputTopic(inputSsmTopicName);
        return parameters;
    }



    private ProcessedSrm createSrm(ZonedDateTime timestamp) {
        var geometry = new Point(lon, lat);
        var props = new SrmProperties();
        props.setTimeStamp(timestamp);
        props.setOdeReceivedAt(timestamp.plusSeconds(secondsLatency));
        props.setLongitude(lon);
        props.setLatitude(lat);
        props.setVehicleID(vehicleId);
        props.setRole(vehicleType);
        var requests = new ArrayList<ProcessedSignalRequest>();
        var request = new ProcessedSignalRequest();
        request.setPriorityRequestType(requestType);
        request.setRequestID(requestId);
        request.setIntersectionId(intersectionId);
        request.setRegion(roadRegulatorId);
        request.setInboundLaneID(inboundLaneId);
        request.setOutboundLaneID(outboundLaneId);
        requests.add(request);
        props.setRequests(requests);
        return new ProcessedSrm(geometry, props);
    }

    private ProcessedSsm createSsm(final ZonedDateTime timestamp, final ProcessedPrioritizationResponseStatus signalStatus) {
        var ssm =  new ProcessedSsm();
        ssm.setIntersectionId(intersectionId);
        ssm.setRegion(roadRegulatorId);
        ssm.setTimeStamp(timestamp);
        ssm.setOdeReceivedAt(timestamp.plusSeconds(secondsLatency));
        var statusList = new ArrayList<ProcessedSignalStatus>();
        var status = new ProcessedSignalStatus();
        status.setRequestID(requestId);
        status.setVehicleID(vehicleId);
        status.setInboundOnLaneID(inboundLaneId);
        status.setOutboundOnLaneID(outboundLaneId);
        status.setRequesterRole(vehicleType);
        status.setStatus(signalStatus);
        statusList.add(status);
        ssm.setStatusList(statusList);
        return ssm;
    }


}
