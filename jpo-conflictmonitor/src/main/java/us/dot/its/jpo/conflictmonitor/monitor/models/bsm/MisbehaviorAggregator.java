package us.dot.its.jpo.conflictmonitor.monitor.models.bsm;
import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.conflictmonitor.monitor.utils.CoordinateConversion;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;




@Getter
@Setter
public class MisbehaviorAggregator {


    private double lateralAcceleration = 0;
    private double longitudinalAcceleration = 0;
    private double verticalAcceleration = 0;
    private int numLateral = 0;
    private int numLongitudinal = 0;
    private int numVertical = 0;
    private long firstRecordTime = 0;
    private long lastRecordTime = 0;
    private String vehicleId = "";
    private double vehicleSpeed = 0;
    private double calculatedSpeed = 0;
    private double calculatedYawRate = 0;
    private double yawRate = 0;
    private double longitude = 0;
    private double latitude = 0;
    private double heading = 0;
    private int numEvents = 0;

    
    public MisbehaviorAggregator add(ProcessedBsm<Point> newBsm) {
        numEvents +=1;

        if(firstRecordTime == 0){
            vehicleId = newBsm.getProperties().getId();
            firstRecordTime = newBsm.getProperties().getTimeStamp().toInstant().toEpochMilli();
        }
        

        double speed = newBsm.getProperties().getSpeed();
        double lat = newBsm.getProperties().getAccelSet().getAccelLat();
        double lng = newBsm.getProperties().getAccelSet().getAccelLong();
        double vert = newBsm.getProperties().getAccelSet().getAccelVert();
        double updatedHeading = newBsm.getProperties().getHeading();
        yawRate = newBsm.getProperties().getAccelSet().getAccelYaw();


        double timeDelta = getDecimalTime(newBsm.getProperties().getTimeStamp().toInstant().toEpochMilli() - lastRecordTime);

        if(newBsm.getGeometry() != null && newBsm.getGeometry().getCoordinates() != null){
            double newLongitude =  newBsm.getGeometry().getCoordinates()[0];
            double newLatitude = newBsm.getGeometry().getCoordinates()[1];

            if(longitude != 0 && latitude != 0){
                double distance = CoordinateConversion.calculateGeodeticDistance(
                    newLatitude,
                    newLongitude,
                    latitude,
                    longitude
                );

                

                calculatedSpeed = (distance / timeDelta) * 2.236936;
            }
            latitude = newLatitude;
            longitude = newLongitude;
        }

        
        if(heading != 0 && updatedHeading != 28800){
            calculatedYawRate = (updatedHeading - heading) / timeDelta;
        }

        heading = updatedHeading;
        
        
        if(speed != 8191){
            vehicleSpeed = getVehicleSpeed(speed);
        }

        if(lat != 2001){
            lateralAcceleration += getVehicleAcceleration(lat);
            numLateral +=1;
        }

        if(lng != 2001){
            longitudinalAcceleration += getVehicleAcceleration(lng);
            numLongitudinal +=1;
        }

        if(vert != -127){
            verticalAcceleration += getVehicleAcceleration(vert);
            numVertical +=1;
        }

        lastRecordTime = newBsm.getProperties().getTimeStamp().toInstant().toEpochMilli();

        return this;
    }

    public double getAverageLateralAcceleration(){
        if(numLateral == 0){
            return 0;
        }
        return lateralAcceleration / numLateral;
    }

    public double getAverageLongitudinalAcceleration(){
        if(numLongitudinal == 0){
            return 0;
        }
        return longitudinalAcceleration / numLongitudinal;
    }

    public double getAverageVerticalAcceleration(){
        if(numVertical == 0){
            return 0;
        }
        return verticalAcceleration / numVertical;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = DateJsonMapper.getInstance();
        String testReturn = "";
        try {
            testReturn = (mapper.writeValueAsString(this));
        } catch (JsonProcessingException e) {
            System.out.println(e);
        }
        return testReturn;
    }

    /**
     * 
     * @param acceleration
     * @return The Vehicle Acceleration converted to Ft / S^2
     */
    public double getVehicleAcceleration(double acceleration){
        return acceleration * 3.2808399; // Already converted to Meters / second
    }

    /**
     * 
     * @param speed
     * @return The Vehicle Acceleration converted to mph
     */
    public double getVehicleSpeed(double speed){
        return speed * 0.6213712; // Speed is already partially converted to M/S in processed BSM
    }

    public double getDecimalTime(long time){
        Instant dt = Instant.ofEpochMilli(time);
        return dt.getEpochSecond() + (((double)dt.getNano()) / 1E9);
    }

}


