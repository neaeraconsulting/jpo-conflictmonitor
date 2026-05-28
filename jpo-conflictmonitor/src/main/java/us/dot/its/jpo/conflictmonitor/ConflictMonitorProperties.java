/*******************************************************************************
 * Copyright 2018 572682
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package us.dot.its.jpo.conflictmonitor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;

import org.apache.commons.lang3.SystemUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import lombok.Getter;
import lombok.Setter;
import lombok.AccessLevel;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.AggregationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.EventAlgorithmMap;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.bsm_message_count_progression.BsmMessageCountProgressionAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.event_state_progression.EventStateProgressionAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.map_message_count_progression.MapMessageCountProgressionAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.map_spat_message_assessment.IntersectionReferenceAlignmentAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.map_spat_message_assessment.SignalGroupAlignmentAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.map_spat_message_assessment.SignalStateConflictAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.spat_message_count_progression.SpatMessageCountProgressionAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.time_change_details.TimeChangeDetailsAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.map.MapMinimumDataAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm.RtcmMinimumDataAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.spat.SpatMinimumDataAggregationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.bsm_event.BsmEventAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.bsm_event.BsmEventParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.connection_of_travel.ConnectionOfTravelAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.connection_of_travel.ConnectionOfTravelParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.connection_of_travel_assessment.ConnectionOfTravelAssessmentAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.connection_of_travel_assessment.ConnectionOfTravelAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event.EventAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event.EventParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.intersection_event.IntersectionEventAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.lane_direction_of_travel.LaneDirectionOfTravelAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.lane_direction_of_travel.LaneDirectionOfTravelParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.lane_direction_of_travel_assessment.LaneDirectionOfTravelAssessmentAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.lane_direction_of_travel_assessment.LaneDirectionOfTravelAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_spat_message_assessment.MapSpatMessageAssessmentAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_spat_message_assessment.MapSpatMessageAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.notification.NotificationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.notification.NotificationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.message_ingest.MessageIngestAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.message_ingest.MessageIngestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_message_count_progression.MapMessageCountProgressionAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_message_count_progression.MapMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression.SpatMessageCountProgressionAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression.SpatMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.bsm_message_count_progression.BsmMessageCountProgressionAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.bsm_message_count_progression.BsmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event_state_progression.EventStateProgressionAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event_state_progression.EventStateProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_passage.StopLinePassageAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_passage.StopLinePassageParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_passage_assessment.StopLinePassageAssessmentAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_passage_assessment.StopLinePassageAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_stop.StopLineStopAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_stop.StopLineStopParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_stop_assessment.StopLineStopAssessmentAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.stop_line_stop_assessment.StopLineStopAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.map.MapTimeChangeDetailsAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.map.MapTimeChangeDetailsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.spat.SpatTimeChangeDetailsAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.spat.SpatTimeChangeDetailsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.map.MapTimestampDeltaAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.map.MapTimestampDeltaParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.map.MapValidationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.map.MapValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationStreamsAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.*;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.MapMinimumDataEventAggregation;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEventAggregation;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.SpatMinimumDataEventAggregation;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentEventAggregation;

@Getter
@Setter
@ConfigurationProperties
public class ConflictMonitorProperties implements EnvironmentAware  {

   private static final Logger logger = LoggerFactory.getLogger(ConflictMonitorProperties.class);


   @Autowired
   @Setter(AccessLevel.NONE)
   private Environment env;

   private AggregationParameters aggregationParameters;
   private String spatMinimumDataAggregationAlgorithm;
   private SpatMinimumDataAggregationAlgorithmFactory spatMinimumDataAggregationAlgorithmFactory;
   private String mapMinimumDataAggregationAlgorithm;
   private MapMinimumDataAggregationAlgorithmFactory mapMinimumDataAggregationAlgorithmFactory;
   private String rtcmMinimumDataAggregationAlgorithm;
   private RtcmMinimumDataAggregationAlgorithmFactory rtcmMinimumDataAggregationAlgorithmFactory;
   private String eventStateProgressionAggregationAlgorithm;
   private EventStateProgressionAggregationAlgorithmFactory eventStateProgressionAggregationAlgorithmFactory;
   private String intersectionReferenceAlignmentAggregationAlgorithm;
   private IntersectionReferenceAlignmentAggregationAlgorithmFactory intersectionReferenceAlignmentAggregationAlgorithmFactory;
   private String signalGroupAlignmentAggregationAlgorithm;
   private SignalGroupAlignmentAggregationAlgorithmFactory signalGroupAlignmentAggregationAlgorithmFactory;
   private String signalStateConflictAggregationAlgorithm;
   private SignalStateConflictAggregationAlgorithmFactory signalStateConflictAggregationAlgorithmFactory;
   private String timeChangeDetailsAggregationAlgorithm;
   private TimeChangeDetailsAggregationAlgorithmFactory timeChangeDetailsAggregationAlgorithmFactory;
   private String bsmMessageCountProgressionAggregationAlgorithm;
   private BsmMessageCountProgressionAggregationAlgorithmFactory bsmMessageCountProgressionAggregationAlgorithmFactory;
   private String mapMessageCountProgressionAggregationAlgorithm;
   private MapMessageCountProgressionAggregationAlgorithmFactory mapMessageCountProgressionAggregationAlgorithmFactory;
   private String spatMessageCountProgressionAggregationAlgorithm;
   private SpatMessageCountProgressionAggregationAlgorithmFactory spatMessageCountProgressionAggregationAlgorithmFactory;
   private String rtcmMessageCountProgressionAggregationAlgorithm;
   private RtcmMessageCountProgressionAggregationAlgorithmFactory rtcmMessageCountProgressionAggregationAlgorithmFactory;
   private String revocableEnabledLaneAlignmentAggregationAlgorithm;
   private RevocableEnabledLaneAlignmentAggregationAlgorithmFactory revocableEnabledLaneAlignmentAggregationAlgorithmFactory;

   private MapValidationAlgorithmFactory mapValidationAlgorithmFactory;
   private SpatValidationStreamsAlgorithmFactory spatValidationAlgorithmFactory;
   private RtcmValidationAlgorithmFactory rtcmValidationAlgorithmFactory;
   private String mapValidationAlgorithm;
   private String spatValidationAlgorithm;
   private String rtcmValidationAlgorithm;
   private SpatValidationParameters spatValidationParameters;
   private MapValidationParameters mapValidationParameters;
   private RtcmValidationParameters rtcmValidationParameters;

   private String mapTimestampDeltaAlgorithm;
   private MapTimestampDeltaParameters mapTimestampDeltaParameters;
   private MapTimestampDeltaAlgorithmFactory mapTimestampDeltaAlgorithmFactory;

   private String spatTimestampDeltaAlgorithm;
   private SpatTimestampDeltaParameters spatTimestampDeltaParameters;
   private SpatTimestampDeltaAlgorithmFactory spatTimestampDeltaAlgorithmFactory;

   private LaneDirectionOfTravelAlgorithmFactory laneDirectionOfTravelAlgorithmFactory;
   private String laneDirectionOfTravelAlgorithm;
   private LaneDirectionOfTravelParameters laneDirectionOfTravelParameters;
   
   private ConnectionOfTravelAlgorithmFactory connectionOfTravelAlgorithmFactory;
   private String connectionOfTravelAlgorithm;
   private ConnectionOfTravelParameters connectionOfTravelParameters;

   private StopLinePassageAlgorithmFactory signalStateVehicleCrossesAlgorithmFactory;
   private String signalStateVehicleCrossesAlgorithm;
   private StopLinePassageParameters signalStateVehicleCrossesParameters;

   private StopLineStopAlgorithmFactory signalStateVehicleStopsAlgorithmFactory;
   private String signalStateVehicleStopsAlgorithm;
   private StopLineStopParameters signalStateVehicleStopsParameters;

   private MapSpatMessageAssessmentAlgorithmFactory mapSpatMessageAssessmentAlgorithmFactory;
   private String mapSpatMessageAssessmentAlgorithm;
   private MapSpatMessageAssessmentParameters mapSpatMessageAssessmentParameters;

   private SpatTimeChangeDetailsAlgorithmFactory spatTimeChangeDetailsAlgorithmFactory;
   private String spatTimeChangeDetailsAlgorithm;
   private String spatTimeChangeDetailsNotificationAlgorithm;
   private SpatTimeChangeDetailsParameters spatTimeChangeDetailsParameters;

   private EventStateProgressionAlgorithmFactory spatTransitionAlgorithmFactory;
   private String spatTransitionAlgorithm;
   private EventStateProgressionParameters spatTransitionParameters;

   private MapTimeChangeDetailsAlgorithmFactory mapTimeChangeDetailsAlgorithmFactory;
   private String mapTimeChangeDetailsAlgorithm;
   private MapTimeChangeDetailsParameters mapTimeChangeDetailsParameters;

   private StopLinePassageAssessmentAlgorithmFactory stopLinePassageAssessmentAlgorithmFactory;
   private String stopLinePassageAssessmentAlgorithm;
   private StopLinePassageAssessmentParameters stopLinePassageAssessmentAlgorithmParameters;

   private StopLineStopAssessmentAlgorithmFactory stopLineStopAssessmentAlgorithmFactory;
   private String stopLineStopAssessmentAlgorithm;
   private StopLineStopAssessmentParameters stopLineStopAssessmentAlgorithmParameters;

   private LaneDirectionOfTravelAssessmentAlgorithmFactory laneDirectionOfTravelAssessmentAlgorithmFactory;
   private String laneDirectionOfTravelAssessmentAlgorithm;
   private LaneDirectionOfTravelAssessmentParameters laneDirectionOfTravelAssessmentAlgorithmParameters;

   private ConnectionOfTravelAssessmentAlgorithmFactory connectionOfTravelAssessmentAlgorithmFactory;
   private String connectionOfTravelAssessmentAlgorithm;
   private ConnectionOfTravelAssessmentParameters connectionOfTravelAssessmentAlgorithmParameters;

   private MapMessageCountProgressionAlgorithmFactory mapMessageCountProgressionAlgorithmFactory;
   private String mapMessageCountProgressionAlgorithm;
   private MapMessageCountProgressionParameters mapMessageCountProgressionAlgorithmParameters;

   private SpatMessageCountProgressionAlgorithmFactory spatMessageCountProgressionAlgorithmFactory;
   private String spatMessageCountProgressionAlgorithm;
   private SpatMessageCountProgressionParameters spatMessageCountProgressionAlgorithmParameters;

   private BsmMessageCountProgressionAlgorithmFactory bsmMessageCountProgressionAlgorithmFactory;
   private String bsmMessageCountProgressionAlgorithm;
   private BsmMessageCountProgressionParameters bsmMessageCountProgressionAlgorithmParameters;

   private RtcmMessageCountProgressionAlgorithmFactory rtcmMessageCountProgressionAlgorithmFactory;
   private String rtcmMessageCountProgressionAlgorithm;
   private RtcmMessageCountProgressionParameters rtcmMessageCountProgressionAlgorithmParameters;

   private RevocableEnabledLaneAlignmentAlgorithmFactory revocableEnabledLaneAlignmentAlgorithmFactory;
   private String revocableEnabledLaneAlignmentAlgorithm;
   private RevocableEnabledLaneAlignmentParameters revocableEnabledLaneAlignmentParameters;

   private PriorityPreemptionRequestAlgorithmFactory priorityPreemptionRequestAlgorithmFactory;
   private String priorityPreemptionRequestAlgorithm;
   private PriorityPreemptionRequestParameters priorityPreemptionRequestParameters;

   private CommonMetricsParameters commonMetricsParameters;
   private PriorityRequestMetricsAlgorithmFactory priorityRequestMetricsAlgorithmFactory;
   private String priorityRequestMetricsAlgorithm;
   private PriorityRequestMetricsParameters priorityRequestMetricsParameters;

   private NotificationAlgorithmFactory notificationAlgorithmFactory;
   private String notificationAlgorithm;
   private NotificationParameters notificationAlgorithmParameters;

   private EventAlgorithmFactory eventAlgorithmFactory;
   private String eventAlgorithm;
   private EventParameters eventParameters;

   // Confluent Properties
   private boolean confluentCloudEnabled = false;
   private String confluentKey = null;
   private String confluentSecret = null;

   private int lingerMs = 0;


   

   private IntersectionEventAlgorithmFactory intersectionEventAlgorithmFactory;
   private String intersectionEventAlgorithm;

   
   private String kafkaStateChangeEventTopic;


   private String appHealthNotificationTopic;

   private BsmEventAlgorithmFactory bsmEventAlgorithmFactory;
   private BsmEventParameters bsmEventParameters;

   private MessageIngestAlgorithmFactory messageIngestAlgorithmFactory;
   private MessageIngestParameters messageIngestParameters;

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setAggregationParameters(AggregationParameters aggregationParameters) {
      this.aggregationParameters = aggregationParameters;
      EventAlgorithmMap algorithmMap = aggregationParameters.getEventAlgorithmMap();
      this.spatMinimumDataAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, SpatMinimumDataEventAggregation.class);
      this.mapMinimumDataAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, MapMinimumDataEventAggregation.class);
      this.eventStateProgressionAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, EventStateProgressionEventAggregation.class);
      this.intersectionReferenceAlignmentAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, IntersectionReferenceAlignmentEventAggregation.class);
      this.signalGroupAlignmentAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, SignalGroupAlignmentEventAggregation.class);
      this.signalStateConflictAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, SignalStateConflictEventAggregation.class);
      this.timeChangeDetailsAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, TimeChangeDetailsEventAggregation.class);
      this.bsmMessageCountProgressionAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, BsmMessageCountProgressionEventAggregation.class);
      this.mapMessageCountProgressionAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, MapMessageCountProgressionEventAggregation.class);
      this.spatMessageCountProgressionAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, SpatMessageCountProgressionEventAggregation.class);
      this.rtcmMessageCountProgressionAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, RtcmMessageCountProgressionEventAggregation.class);
      this.revocableEnabledLaneAlignmentAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, RevocableEnabledLaneAlignmentEventAggregation.class);
      this.rtcmMinimumDataAggregationAlgorithm
              = getAlgorithmFromMap(algorithmMap, RtcmMinimumDataEventAggregation.class);
   }

   // Get algorithm name from map of event type to algorithm
   private <TAggEvent extends EventAggregation<?>> String getAlgorithmFromMap(
                 EventAlgorithmMap algorithmMap,
                 Class<TAggEvent> aggEventClass) {
      String eventType = "";
       try {
           Constructor<TAggEvent> cons = aggEventClass.getConstructor();
           eventType = cons.newInstance().getEventType();
       } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
           logger.error("Exception getting event type", e);
       }
      if (algorithmMap.containsKey(eventType)) {
         return algorithmMap.get(eventType);
      } else {
         throw new RuntimeException("No algorithm found for " + eventType);
      }
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatMinimumDataAggregationAlgorithmFactory(SpatMinimumDataAggregationAlgorithmFactory factory) {
      this.spatMinimumDataAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapMinimumDataAggregationAlgorithmFactory(MapMinimumDataAggregationAlgorithmFactory factory) {
      this.mapMinimumDataAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRtcmMinimumDataAggregationAlgorithmFactory(RtcmMinimumDataAggregationAlgorithmFactory factory) {
      this.rtcmMinimumDataAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setEventStateProgressionAggregationAlgorithmFactory(EventStateProgressionAggregationAlgorithmFactory factory) {
      this.eventStateProgressionAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setIntersectionReferenceAlignmentAggregationAlgorithmFactory(IntersectionReferenceAlignmentAggregationAlgorithmFactory factory) {
      this.intersectionReferenceAlignmentAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSignalGroupAlignmentAggregationAlgorithmFactory(SignalGroupAlignmentAggregationAlgorithmFactory factory) {
      this.signalGroupAlignmentAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSignalStateConflictAggregationAlgorithmFactory(SignalStateConflictAggregationAlgorithmFactory factory) {
      this.signalStateConflictAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setTimeChangeDetailsAggregationAlgorithmFactory(TimeChangeDetailsAggregationAlgorithmFactory factory) {
      this.timeChangeDetailsAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setBsmMessageCountProgressionAggregationAlgorithmFactory(BsmMessageCountProgressionAggregationAlgorithmFactory factory) {
      this.bsmMessageCountProgressionAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapMessageCountProgressionAggregationAlgorithmFactory(MapMessageCountProgressionAggregationAlgorithmFactory factory) {
      this.mapMessageCountProgressionAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatMessageCountProgressionAggregationAlgorithmFactory(SpatMessageCountProgressionAggregationAlgorithmFactory factory) {
      this.spatMessageCountProgressionAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRtcmMessageCountProgressionAggregationAlgorithmFactory(RtcmMessageCountProgressionAggregationAlgorithmFactory factory) {
      this.rtcmMessageCountProgressionAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRevocableEnabledLaneAlignmentAggregationAlgorithmFactory(RevocableEnabledLaneAlignmentAggregationAlgorithmFactory factory) {
      this.revocableEnabledLaneAlignmentAggregationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setIntersectionEventAlgorithmFactory(IntersectionEventAlgorithmFactory intersectionEventAlgorithmFactory) {
      this.intersectionEventAlgorithmFactory = intersectionEventAlgorithmFactory;
   }



   @Value("${intersection.event.algorithm}")
   public void setIntersectionEventAlgorithm(String intersectionEventAlgorithm) {
      this.intersectionEventAlgorithm = intersectionEventAlgorithm;
   }

   @Autowired
   public void setMapValidationParameters(MapValidationParameters mapBroadcastRateParameters) {
      this.mapValidationParameters = mapBroadcastRateParameters;
   }

   @Autowired
   public void setSpatValidationParameters(SpatValidationParameters spatBroadcastRateParameters) {
      this.spatValidationParameters = spatBroadcastRateParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRtcmValidationParameters(RtcmValidationParameters rtcmValidationParameters) {
      this.rtcmValidationParameters = rtcmValidationParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapValidationAlgorithmFactory(MapValidationAlgorithmFactory factory) {
      this.mapValidationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatValidationAlgorithmFactory(SpatValidationStreamsAlgorithmFactory factory) {
      this.spatValidationAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRtcmValidationAlgorithmFactory(RtcmValidationAlgorithmFactory factory) {
      this.rtcmValidationAlgorithmFactory = factory;
   }

   @Value("${map.validation.algorithm}")
   public void setMapValidationAlgorithm(String mapBroadcastRateAlgorithm) {
      this.mapValidationAlgorithm = mapBroadcastRateAlgorithm;
   }

   @Value("${spat.validation.algorithm}")
   public void setSpatValidationAlgorithm(String spatBroadcastRateAlgorithm) {
      this.spatValidationAlgorithm = spatBroadcastRateAlgorithm;
   }

   @Value("${rtcm.validation.algorithm}")
   public void setRtcmValidationAlgorithm(String rtcmValidationAlgorithm) {
      this.rtcmValidationAlgorithm = rtcmValidationAlgorithm;
   }


   @Autowired
   public void setMapTimestampDeltaParameters(MapTimestampDeltaParameters mapTimestampDeltaParameters) {
      this.mapTimestampDeltaParameters = mapTimestampDeltaParameters;
      this.mapTimestampDeltaAlgorithm = mapTimestampDeltaParameters.getAlgorithm();
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapTimestampDeltaAlgorithmFactory(MapTimestampDeltaAlgorithmFactory factory) {
      this.mapTimestampDeltaAlgorithmFactory = factory;
   }


   @Autowired
   public void setSpatTimestampDeltaParameters(SpatTimestampDeltaParameters spatTimestampDeltaParameters) {
      this.spatTimestampDeltaParameters = spatTimestampDeltaParameters;
      this.spatTimestampDeltaAlgorithm = spatTimestampDeltaParameters.getAlgorithm();
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatTimestampDeltaAlgorithmFactory(SpatTimestampDeltaAlgorithmFactory factory) {
      this.spatTimestampDeltaAlgorithmFactory = factory;
   }

   @Autowired
   public void setRevocableEnabledLaneAlignmentParameters(RevocableEnabledLaneAlignmentParameters revocableEnabledLaneAlignmentParameters) {
      this.revocableEnabledLaneAlignmentParameters = revocableEnabledLaneAlignmentParameters;
      this.revocableEnabledLaneAlignmentAlgorithm = revocableEnabledLaneAlignmentParameters.getAlgorithm();
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRevocableEnabledLaneAlignmentAlgorithmFactory(RevocableEnabledLaneAlignmentAlgorithmFactory factory) {
      this.revocableEnabledLaneAlignmentAlgorithmFactory = factory;
   }
   @Autowired
   public void setPriorityPreemptionRequestParameters(PriorityPreemptionRequestParameters priorityPreemptionRequestParameters) {
      this.priorityPreemptionRequestParameters = priorityPreemptionRequestParameters;
      this.priorityPreemptionRequestAlgorithm = priorityPreemptionRequestParameters.getAlgorithm();
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setPriorityRequestMetricsParameters(PriorityRequestMetricsParameters priorityRequestMetricsParameters) {
      this.priorityRequestMetricsParameters = priorityRequestMetricsParameters;
      this.priorityRequestMetricsAlgorithm = priorityRequestMetricsParameters.getAlgorithm();
   }

   @Autowired
   public void setCommonMetricsParameters(CommonMetricsParameters commonMetricsParameters) {
      this.commonMetricsParameters = commonMetricsParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setPriorityPreemptionRequestAlgorithmFactory(PriorityPreemptionRequestAlgorithmFactory factory) {
      this.priorityPreemptionRequestAlgorithmFactory = factory;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setPriorityRequestMetricsAlgorithmFactory(PriorityRequestMetricsAlgorithmFactory factory) {
      this.priorityRequestMetricsAlgorithmFactory = factory;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setLaneDirectionOfTravelAlgorithmFactory(
         LaneDirectionOfTravelAlgorithmFactory laneDirectionOfTravelAlgorithmFactory) {
      this.laneDirectionOfTravelAlgorithmFactory = laneDirectionOfTravelAlgorithmFactory;
   }



   @Value("${lane.direction.of.travel.algorithm}")
   public void setLaneDirectionOfTravelAlgorithm(String laneDirectionOfTravelAlgorithm) {
      this.laneDirectionOfTravelAlgorithm = laneDirectionOfTravelAlgorithm;
   }



   @Autowired
   public void setLaneDirectionOfTravelParameters(LaneDirectionOfTravelParameters laneDirectionOfTravelParameters) {
      this.laneDirectionOfTravelParameters = laneDirectionOfTravelParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setConnectionOfTravelAlgorithmFactory(
         ConnectionOfTravelAlgorithmFactory connectionOfTravelAlgorithmFactory) {
      this.connectionOfTravelAlgorithmFactory = connectionOfTravelAlgorithmFactory;
   }



   @Value("${connection.of.travel.algorithm}")
   public void setConnectionOfTravelAlgorithm(String connectionOfTravelAlgorithm) {
      this.connectionOfTravelAlgorithm = connectionOfTravelAlgorithm;
   }


   
   @Autowired
   public void setConnectionOfTravelParameters(ConnectionOfTravelParameters connectionOfTravelParameters) {
      this.connectionOfTravelParameters = connectionOfTravelParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSignalStateVehicleCrossesAlgorithmFactory(
         StopLinePassageAlgorithmFactory signalStateVehicleCrossesAlgorithmFactory) {
      this.signalStateVehicleCrossesAlgorithmFactory = signalStateVehicleCrossesAlgorithmFactory;
   }
   


   @Value("${signal.state.vehicle.crosses.algorithm}")
   public void setSignalStateVehicleCrossesAlgorithm(String signalStateVehicleCrossesAlgorithm) {
      this.signalStateVehicleCrossesAlgorithm = signalStateVehicleCrossesAlgorithm;
   }



   @Autowired
   public void setSignalStateVehicleCrossesParameters(
         StopLinePassageParameters signalStateVehicleCrossesParameters) {
      this.signalStateVehicleCrossesParameters = signalStateVehicleCrossesParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSignalStateVehicleStopsAlgorithmFactory(
         StopLineStopAlgorithmFactory signalStateVehicleStopsAlgorithmFactory) {
      this.signalStateVehicleStopsAlgorithmFactory = signalStateVehicleStopsAlgorithmFactory;
   }



   @Value("${signal.state.vehicle.stops.algorithm}")
   public void setSignalStateVehicleStopsAlgorithm(String signalStateVehicleStopsAlgorithm) {
      this.signalStateVehicleStopsAlgorithm = signalStateVehicleStopsAlgorithm;
   }



   @Autowired
   public void setSignalStateVehicleStopsParameters(StopLineStopParameters signalStateVehicleStopsParameters) {
      this.signalStateVehicleStopsParameters = signalStateVehicleStopsParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapSpatMessageAssessmentAlgorithmFactory(
         MapSpatMessageAssessmentAlgorithmFactory mapSpatMessageAssessmentAlgorithmFactory) {
      this.mapSpatMessageAssessmentAlgorithmFactory = mapSpatMessageAssessmentAlgorithmFactory;
   }
   
   @Value("${map.spat.message.assessment.algorithm}")
   public void setMapSpatMessageAssessmentAlgorithm(String mapSpatMessageAssessmentAlgorithm) {
      this.mapSpatMessageAssessmentAlgorithm = mapSpatMessageAssessmentAlgorithm;
   }



   @Autowired
   public void setMapSpatMessageAssessmentParameters(
         MapSpatMessageAssessmentParameters mapSpatMessageAssessmentParameters) {
      this.mapSpatMessageAssessmentParameters = mapSpatMessageAssessmentParameters;
   }



   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatTimeChangeDetailsAlgorithmFactory(
         SpatTimeChangeDetailsAlgorithmFactory spatTimeChangeDetailsAlgorithmFactory) {
      this.spatTimeChangeDetailsAlgorithmFactory = spatTimeChangeDetailsAlgorithmFactory;
   }



   @Value("${spat.time.change.details.algorithm}")
   public void setSpatTimeChangeDetailsAlgorithm(String spatTimeChangeDetailsAlgorithm) {
      this.spatTimeChangeDetailsAlgorithm = spatTimeChangeDetailsAlgorithm;
   }



   @Value("${spat.time.change.details.notification.algorithm}")
   public void setSpatTimeChangeDetailsNotificationAlgorithm(String spatTimeChangeDetailsNotificationAlgorithm) {
      this.spatTimeChangeDetailsNotificationAlgorithm = spatTimeChangeDetailsNotificationAlgorithm;
   }



   @Autowired
   public void setSpatTimeChangeDetailsParameters(SpatTimeChangeDetailsParameters spatTimeChangeDetailsParameters) {
      this.spatTimeChangeDetailsParameters = spatTimeChangeDetailsParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatTransitionAlgorithmFactory(EventStateProgressionAlgorithmFactory spatTransitionAlgorithmFactory) {
      this.spatTransitionAlgorithmFactory = spatTransitionAlgorithmFactory;
   }

   @Value("${event.state.progression.algorithm}")
   public void setSpatTransitionAlgorithm(String spatTransitionAlgorithm) {
      this.spatTransitionAlgorithm = spatTransitionAlgorithm;
   }

   @Autowired
   public void setSpatTransitionParameters(EventStateProgressionParameters spatTransitionParameters) {
      this.spatTransitionParameters = spatTransitionParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapTimeChangeDetailsAlgorithmFactory(
         MapTimeChangeDetailsAlgorithmFactory mapTimeChangeDetailsAlgorithmFactory) {
      this.mapTimeChangeDetailsAlgorithmFactory = mapTimeChangeDetailsAlgorithmFactory;
   }



   @Value("${map.time.change.details.algorithm}")
   public void setMapTimeChangeDetailsAlgorithm(String mapTimeChangeDetailsAlgorithm) {
      this.mapTimeChangeDetailsAlgorithm = mapTimeChangeDetailsAlgorithm;
   }



   @Autowired
   public void setMapTimeChangeDetailsParameters(MapTimeChangeDetailsParameters mapTimeChangeDetailsParameters) {
      this.mapTimeChangeDetailsParameters = mapTimeChangeDetailsParameters;
   }



   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setStopLinePassageAssessmentAlgorithmFactory(
         StopLinePassageAssessmentAlgorithmFactory stopLinePassageAssessmentAlgorithmFactory) {
      this.stopLinePassageAssessmentAlgorithmFactory = stopLinePassageAssessmentAlgorithmFactory;
   }

 

   @Value("${stop.line.passage.assessment.algorithm}")
   public void setStopLinePassageAssessmentAlgorithm(String stopLinePassageAssessmentAlgorithm) {
      this.stopLinePassageAssessmentAlgorithm = stopLinePassageAssessmentAlgorithm;
   }



   @Autowired
   public void setStopLinePassageAssessmentAlgorithmParameters(
      StopLinePassageAssessmentParameters stopLinePassageAssessmentAlgorithmParameters) {
      this.stopLinePassageAssessmentAlgorithmParameters = stopLinePassageAssessmentAlgorithmParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setStopLineStopAssessmentAlgorithmFactory(
         StopLineStopAssessmentAlgorithmFactory stopLineStopAssessmentAlgorithmFactory) {
      this.stopLineStopAssessmentAlgorithmFactory = stopLineStopAssessmentAlgorithmFactory;
   }

 

   @Value("${stop.line.stop.assessment.algorithm}")
   public void setStopLineStopAssessmentAlgorithm(String stopLineStopAssessmentAlgorithm) {
      this.stopLineStopAssessmentAlgorithm = stopLineStopAssessmentAlgorithm;
   }



   @Autowired
   public void setStopLineStopAssessmentAlgorithmParameters(
      StopLineStopAssessmentParameters stopLineStopAssessmentAlgorithmParameters) {
      this.stopLineStopAssessmentAlgorithmParameters = stopLineStopAssessmentAlgorithmParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setLaneDirectionOfTravelAssessmentAlgorithmFactory(
         LaneDirectionOfTravelAssessmentAlgorithmFactory laneDirectionfOfTravelAssessmentAlgorithmFactory) {
      this.laneDirectionOfTravelAssessmentAlgorithmFactory = laneDirectionfOfTravelAssessmentAlgorithmFactory;
   }



   @Value("${lane.direction.of.travel.assessment.algorithm}")
   public void setLaneDirectionOfTravelAssessmentAlgorithm(String laneDirectionOfTravelAssessmentAlgorithm) {
      this.laneDirectionOfTravelAssessmentAlgorithm = laneDirectionOfTravelAssessmentAlgorithm;
   }



   @Autowired
   public void setLaneDirectionOfTravelAssessmentAlgorithmParameters(
         LaneDirectionOfTravelAssessmentParameters laneDirectionOfTravelAssessmentAlgorithmParameters) {
      this.laneDirectionOfTravelAssessmentAlgorithmParameters = laneDirectionOfTravelAssessmentAlgorithmParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setConnectionOfTravelAssessmentAlgorithmFactory(
         ConnectionOfTravelAssessmentAlgorithmFactory connectionOfTravelAssessmentAlgorithmFactory) {
      this.connectionOfTravelAssessmentAlgorithmFactory = connectionOfTravelAssessmentAlgorithmFactory;
   }
   


   @Value("${connection.of.travel.assessment.algorithm}")
   public void setConnectionOfTravelAssessmentAlgorithm(String connectionOfTravelAssessmentAlgorithm) {
      this.connectionOfTravelAssessmentAlgorithm = connectionOfTravelAssessmentAlgorithm;
   }



   @Autowired
   public void setConnectionOfTravelAssessmentAlgorithmParameters(
         ConnectionOfTravelAssessmentParameters connectionOfTravelAssessmentAlgorithmParameters) {
      this.connectionOfTravelAssessmentAlgorithmParameters = connectionOfTravelAssessmentAlgorithmParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMapMessageCountProgressionAlgorithmFactory(MapMessageCountProgressionAlgorithmFactory mapMessageCountProgressionAlgorithmFactory) {
      this.mapMessageCountProgressionAlgorithmFactory = mapMessageCountProgressionAlgorithmFactory;
   }

   @Value("${map.message.count.progression.algorithm}")
   public void setMapRevisionCorrectionAlgorithm(String mapMessageCountProgressionAlgorithm) {
      this.mapMessageCountProgressionAlgorithm = mapMessageCountProgressionAlgorithm;
   }

   @Autowired
   public void setMapMessageCountProgressionAlgorithmParameters(MapMessageCountProgressionParameters mapMessageCountProgressionAlgorithmParameters) {
      this.mapMessageCountProgressionAlgorithmParameters = mapMessageCountProgressionAlgorithmParameters;
   }


   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setSpatMessageCountProgressionAlgorithmFactory(SpatMessageCountProgressionAlgorithmFactory spatMessageCountProgressionAlgorithmFactory) {
      this.spatMessageCountProgressionAlgorithmFactory = spatMessageCountProgressionAlgorithmFactory;
   }

   @Value("${spat.message.count.progression.algorithm}")
   public void setSpatRevisionCorrectionAlgorithm(String spatMessageCountProgressionAlgorithm) {
      this.spatMessageCountProgressionAlgorithm = spatMessageCountProgressionAlgorithm;
   }

   @Autowired
   public void setSpatMessageCountProgressionAlgorithmParameters(SpatMessageCountProgressionParameters spatMessageCountProgressionAlgorithmParameters) {
      this.spatMessageCountProgressionAlgorithmParameters = spatMessageCountProgressionAlgorithmParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setBsmMessageCountProgressionAlgorithmFactory(BsmMessageCountProgressionAlgorithmFactory bsmMessageCountProgressionAlgorithmFactory) {
      this.bsmMessageCountProgressionAlgorithmFactory = bsmMessageCountProgressionAlgorithmFactory;
   }

   @Value("${bsm.message.count.progression.algorithm}")
   public void setBsmRevisionCorrectionAlgorithm(String bsmMessageCountProgressionAlgorithm) {
      this.bsmMessageCountProgressionAlgorithm = bsmMessageCountProgressionAlgorithm;
   }

   @Autowired
   public void setBsmMessageCountProgressionAlgorithmParameters(BsmMessageCountProgressionParameters bsmMessageCountProgressionAlgorithmParameters) {
      this.bsmMessageCountProgressionAlgorithmParameters = bsmMessageCountProgressionAlgorithmParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setRtcmMessageCountProgressionAlgorithmFactory(RtcmMessageCountProgressionAlgorithmFactory factory) {
      this.rtcmMessageCountProgressionAlgorithmFactory = factory;
   }

   @Value("${rtcm.message.count.progression.algorithm}")
   public void setRtcmMessageCountProgressionAlgorithm(String algorithmName) {
      this.rtcmMessageCountProgressionAlgorithm = algorithmName;
   }

   @Autowired
   public void setRtcmMessageCountProgressionAlgorithmParameters(RtcmMessageCountProgressionParameters params) {
      this.rtcmMessageCountProgressionAlgorithmParameters = params;
   }


   public NotificationAlgorithmFactory getNotificationAlgorithmFactory() {
      return notificationAlgorithmFactory;
   }



   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setNotificationAlgorithmFactory(NotificationAlgorithmFactory notificationAlgorithmFactory) {
      this.notificationAlgorithmFactory = notificationAlgorithmFactory;
   }

    @Value("${notification.algorithm}")
   public void setNotificationAlgorithm(String notificationAlgorithm) {
      this.notificationAlgorithm = notificationAlgorithm;
   }

    @Autowired
   public void setNotificationAlgorithmParameters(NotificationParameters notificationAlgorithmParameters) {
      this.notificationAlgorithmParameters = notificationAlgorithmParameters;
   }

   @Autowired
   public void setEventParameters(EventParameters eventParameters) {
      this.eventParameters = eventParameters;
      this.eventAlgorithm = eventParameters.getAlgorithm();
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setEventAlgorithmFactory(EventAlgorithmFactory factory) {
      this.eventAlgorithmFactory = factory;
   }

   public Boolean getConfluentCloudStatus() {
		return confluentCloudEnabled;
	}

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setBsmEventAlgorithmFactory(BsmEventAlgorithmFactory bsmEventAlgorithmFactory) {
      this.bsmEventAlgorithmFactory = bsmEventAlgorithmFactory;
   }

   @Autowired
   public void setBsmEventParameters(BsmEventParameters bsmEventParameters) {
      this.bsmEventParameters = bsmEventParameters;
   }

   @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
   @Autowired
   public void setMessageIngestAlgorithmFactory(MessageIngestAlgorithmFactory messageIngestAlgorithmFactory) {
      this.messageIngestAlgorithmFactory = messageIngestAlgorithmFactory;
   }

   @Autowired
   public void setMessageIngestParameters(MessageIngestParameters messageIngestParameters) {
      this.messageIngestParameters = messageIngestParameters;
   }


   /*
    * General Properties
    */
   private String version;
   
   @Setter(AccessLevel.NONE)
   private String kafkaBrokers = null;
   
   @Setter(AccessLevel.NONE)
   private String hostId;


   @Setter(AccessLevel.NONE)
   private String kafkaBrokerIP = null;

   // BSM
   private String kafkaTopicOdeBsmJson;
   private String kafkaTopicCmBsmEvent;
   private String kafkaTopicBsmRepartition;


   // SPAT
   private String kafkaTopicSpatGeoJson;
   private String kafkaTopicProcessedSpat;


   // MAP
   private String kafkaTopicOdeMapJson;
   private String kafkaTopicMapGeoJson;
   private String kafkaTopicProcessedMap;

   //Vehicle Events
   private String kafkaTopicCmLaneDirectionOfTravelEvent;
   private String kafkaTopicCmConnectionOfTravelEvent;
   private String kafkaTopicCmStopLinePassageEvent;
   private String kafakTopicCmVehicleStopEvent; 

   @Setter(AccessLevel.NONE)
   @Autowired
   BuildProperties buildProperties;

   @PostConstruct
   void initialize() {
      setVersion(buildProperties.getVersion());
      logger.info("groupId: {}", buildProperties.getGroup());
      logger.info("artifactId: {}", buildProperties.getArtifact());
      logger.info("version: {}", version);
      //OdeMsgMetadata.setStaticSchemaVersion(OUTPUT_SCHEMA_VERSION);

      

      String hostname;
      try {
         hostname = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
         // Let's just use a random hostname
         hostname = UUID.randomUUID().toString();
         logger.info("Unknown host error: {}, using random", e);
      }
      hostId = hostname;
      logger.info("Host ID: {}", hostId);
      logger.info("Initializing services on host {}", hostId);

      // No longer need a mongo DB connection for this service
      // if(dbHostIP == null){
      //    String dbHost = CommonUtils.getEnvironmentVariable("MONGO_IP");

      //    if(dbHost == null){
      //       logger.warn(
      //             "DB Host IP not defined, Defaulting to localhost.");
      //       dbHost = "localhost";
      //    }
      //    dbHostIP = dbHost;
      // }

      if (kafkaBrokers == null) {

         String kafkaBrokers = getEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");

         logger.info("ode.kafkaBrokers property not defined. Will try KAFKA_BOOTSTRAP_SERVERS => {}", kafkaBrokers);

         if (kafkaBrokers == null) {
            logger.warn(
                  "Neither ode.kafkaBrokers ode property nor KAFKA_BOOTSTRAP_SERVERS environment variable are defined. Defaulting to localhost:9092");
            kafkaBrokers = "localhost:9092";
         }

      }

      String kafkaType = getEnvironmentVariable("KAFKA_TYPE");
      if (kafkaType != null) {
         confluentCloudEnabled = kafkaType.equals("CONFLUENT");
         if (confluentCloudEnabled) {
               
               System.out.println("Enabling Confluent Cloud Integration");

               confluentKey = getEnvironmentVariable("CONFLUENT_KEY");
               confluentSecret = getEnvironmentVariable("CONFLUENT_SECRET");
         }
      }

      // List<String> asList = Arrays.asList(this.getKafkaTopicsDisabled());
      // logger.info("Disabled Topics: {}", asList);
      // kafkaTopicsDisabledSet.addAll(asList);
   }

   // Streams configurations
   private int streamsConfigReplicationFactor;
   private String streamsConfigAcks;
   private int streamsConfigNumStreamThreads;
   private long streamsConfigCacheMaxBytesBuffering;
   private int streamsConfigCommitIntervalMs;
   private String streamsConfigCompressionType;
   private int streamsConfigLingerMs;
   private int streamsConfigBatchSize;
   private String streamsConfigTopologyOptimization;
   private TopologyStreamsConfig topologyStreamsConfig;

   @Autowired
   public void setTopologyStreamsConfig(TopologyStreamsConfig topologyStreamsConfig) {
      this.topologyStreamsConfig = topologyStreamsConfig;
   }

   public Properties createStreamProperties(String name) {
      Properties streamProps = new Properties();

      streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, name);

      streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);

      streamProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class.getName());

      streamProps.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            LogAndSkipOnInvalidTimestamp.class.getName());

      streamProps.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
            AlwaysContinueProductionExceptionHandler.class.getName());

      streamProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamsConfigNumStreamThreads);
      var threadConfig = topologyStreamsConfig.getTopologies()
              .stream()
              .collect(Collectors.toMap(
                      TopologyStreamsConfig.TopologyConfig::topology, TopologyStreamsConfig.TopologyConfig::threads));
      if (threadConfig.containsKey(name)) {
         int numThreads = threadConfig.get(name);
         logger.info("Using {} threads for {} topology", numThreads, name);
         streamProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
      } else {
         logger.warn("Number of threads for {} topology is not configured," +
                 " using default: {}", name, streamsConfigNumStreamThreads);
         streamProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamsConfigNumStreamThreads);
      }

      streamProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, streamsConfigReplicationFactor);
      streamProps.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), streamsConfigAcks);

      // Reduce cache buffering per topology to 1MB
      streamProps.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, streamsConfigCacheMaxBytesBuffering);
      // Optionally, to disable caching:
      //streamProps.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

      // Decrease default commit interval. Default for 'at least once' mode of 30000ms
      // is too slow.
      streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streamsConfigCommitIntervalMs);

      streamProps.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, streamsConfigTopologyOptimization);

      // Processing Exception handler!!
      // New feature in Kafka 3.9
      // KIP:
      // https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing
      streamProps.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
              LogAndContinueProcessingExceptionHandler.class.getName());

      // All the keys are Strings in this app
      streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

      // Configure the state store location
      if (SystemUtils.IS_OS_LINUX) {
         streamProps.put(StreamsConfig.STATE_DIR_CONFIG, "/var/lib/ode/kafka-streams");
      } else if (SystemUtils.IS_OS_WINDOWS) {
         streamProps.put(StreamsConfig.STATE_DIR_CONFIG, "C:/temp/ode");
      }


      // Disable batching
      // streamProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);

      // Enable Compression
      streamProps.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), streamsConfigCompressionType);
      streamProps.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), streamsConfigLingerMs);
      streamProps.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), streamsConfigBatchSize);

      if (confluentCloudEnabled) {
         streamProps.put("ssl.endpoint.identification.algorithm", "https");
         streamProps.put("security.protocol", "SASL_SSL");
         streamProps.put("sasl.mechanism", "PLAIN");

         if (confluentKey != null && confluentSecret != null) {
             String auth = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                 "username=\"" + confluentKey + "\" " +
                 "password=\"" + confluentSecret + "\";";
                 streamProps.put("sasl.jaas.config", auth);
         }
         else {
             logger.error("Environment variables CONFLUENT_KEY and CONFLUENT_SECRET are not set. Set these in the .env file to use Confluent Cloud");
         }
     }

      // Configure RocksDB memory usage
      streamProps.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class);

      return streamProps;
   }

 

   public String getProperty(String key) {
      return env.getProperty(key);
   }

   public String getProperty(String key, String defaultValue) {
      return env.getProperty(key, defaultValue);
   }

   public Object getProperty(String key, int i) {
      return env.getProperty(key, Integer.class, i);
   }


   @Value("${spring.kafka.bootstrap-servers}")
   public void setKafkaBrokers(String kafkaBrokers) {
      this.kafkaBrokers = kafkaBrokers;
   }

   @Override
   public void setEnvironment(Environment environment) {
      env = environment;
   }

   private static String getEnvironmentVariable(String variableName) {
      String value = System.getenv(variableName);
      if (value == null || value.equals("")) {
          System.out.println("Something went wrong retrieving the environment variable " + variableName);
      }
      return value;
   }


 
}
