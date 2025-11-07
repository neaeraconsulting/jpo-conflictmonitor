package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.vehicle_misbehavior.VehicleMisbehaviorParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.vehicle_misbehavior.VehicleMisbehaviorStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.bsm.MisbehaviorAggregator;
import us.dot.its.jpo.conflictmonitor.monitor.models.bsm.ProcessedBsmTimestampExtractor;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.VehicleMisbehaviorEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.VehicleMisbehaviorReason;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuLogKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.vehicle_misbehavior.VehicleMisbehaviorConstants.DEFAULT_VEHICLE_MISBEHAVIOR_ALGORITHM;

@Component(DEFAULT_VEHICLE_MISBEHAVIOR_ALGORITHM)
@Slf4j
public class VehicleMisbehaviorTopology
        extends BaseStreamsTopology<VehicleMisbehaviorParameters>
        implements VehicleMisbehaviorStreamsAlgorithm {

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();


        KStream<RsuLogKey, ProcessedBsm<Point>> inputStream = builder.stream(parameters.getBsmInputTopicName(),
                Consumed.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuLogKey(),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedBsm()).withTimestampExtractor(new ProcessedBsmTimestampExtractor()));

        KTable<Windowed<RsuLogKey>, MisbehaviorAggregator> accelerations = inputStream
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(2),Duration.ofMillis(500)))
            .aggregate(
                MisbehaviorAggregator::new,
                (key, value, aggregate) -> aggregate.add(value),
                Materialized.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuLogKey(), JsonSerdes.MisbehaviorAggregator()));

        // inputStream.print(Printed.toSysOut());


        KStream<RsuLogKey, VehicleMisbehaviorEvent> vehicleMisbehaviorEventsStream = accelerations
            .toStream()
            .flatMap((key, value)->{
                List<KeyValue<RsuLogKey, VehicleMisbehaviorEvent>> result = new ArrayList<>();
                
                Set<VehicleMisbehaviorReason> misbehaviorReasons = new HashSet<VehicleMisbehaviorReason>();

                if(value.getNumEvents() >=2){
                    if(Math.abs(value.getCalculatedSpeed() - value.getVehicleSpeed()) > parameters.getSpeedRange()){
                        misbehaviorReasons.add(VehicleMisbehaviorReason.SPEED_DELTA_INVALID);
                    } 
                    if(Math.abs(value.getCalculatedYawRate() - value.getYawRate()) > parameters.getYawRateRange()){
                        misbehaviorReasons.add(VehicleMisbehaviorReason.YAW_DELTA_INVALID);
                    }
                }

                if(Math.abs(value.getVehicleSpeed()) > parameters.getAllowableMaxSpeed()){
                    misbehaviorReasons.add(VehicleMisbehaviorReason.EXCESSIVE_SPEED);
                }

                if(Math.abs(value.getYawRate()) > parameters.getAllowableMaxHeadingDelta()){
                    misbehaviorReasons.add(VehicleMisbehaviorReason.EXCESSIVE_ROTATION);
                }

                if(Math.abs(value.getAverageLateralAcceleration()) > parameters.getAccelerationRangeLateral()){
                    misbehaviorReasons.add(VehicleMisbehaviorReason.EXCESSIVE_LATERAL_ACCELERATION);
                }

                if(Math.abs(value.getAverageLongitudinalAcceleration()) > parameters.getAccelerationRangeLongitudinal()){
                    misbehaviorReasons.add(VehicleMisbehaviorReason.EXCESSIVE_LONGITUDINAL_ACCELERATION);
                }

                if(Math.abs(value.getAverageVerticalAcceleration()) > parameters.getAccelerationRangeVertical()){
                    misbehaviorReasons.add(VehicleMisbehaviorReason.EXCESSIVE_VERTICAL_ACCELERATION);
                }

                if(misbehaviorReasons.size() > 0){    
                    VehicleMisbehaviorEvent event = new VehicleMisbehaviorEvent();
                    event.setSource(key.toString());
                    event.setTimeStamp(value.getLastRecordTime());
                    event.setVehicleID(value.getVehicleId());
                    
                    event.setReportedYawRate(value.getYawRate());
                    event.setReportedSpeed(value.getVehicleSpeed());
                    event.setReportedAccelerationLat(value.getAverageLateralAcceleration());
                    event.setReportedAccelerationLon(value.getAverageLongitudinalAcceleration());
                    event.setReportedAccelerationVert(value.getAverageVerticalAcceleration());

                    event.setSpeedRange(parameters.getSpeedRange());
                    event.setAccelerationRangeLat(parameters.getAccelerationRangeLateral());
                    event.setAccelerationRangeLon(parameters.getAccelerationRangeLongitudinal());
                    event.setAccelerationRangeVert(parameters.getAccelerationRangeVertical());

                    event.setCalculatedYawRate(value.getCalculatedYawRate());
                    event.setCalculatedSpeed(value.getCalculatedSpeed());

                    event.setMisbehaviorReasons(misbehaviorReasons);

                    result.add(new KeyValue<RsuLogKey, VehicleMisbehaviorEvent>(key.key(), event));
                }

                return result;
            }
        );

        
        vehicleMisbehaviorEventsStream.to(parameters.getVehicleMisbehaviorEventOutputTopicName(),
                Produced.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuLogKey(),
                        us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes.VehicleMisbehaviorEvent()));


        
        return builder.build();
    }


}