package us.dot.its.jpo.conflictmonitor.monitor.utils;

import java.util.ArrayList;
import java.util.Collections;

public class MathFunctions {
    

    public static double getMedian(ArrayList<Double> list){
        if(list.size() > 0){
            
            Collections.sort(list);

            int middle = list.size()/2;
            if(list.size() % 2 ==0){
                return (list.get(middle) + list.get(middle-1)) / 2.0;
            }else{
                return list.get(list.size()/2);
            }
        } else{
            return 0;
        }
        
    }

    // Computes the median of a list of headings in degrees, correctly handling the 0/360 wrap
    // boundary. A plain getMedian() on angular data breaks when samples straddle 0/360 (e.g.
    // [358, 359, 1, 2] numerically sorts to [1, 2, 358, 359], producing a median of 180 degrees
    // away from the true ~0 degree heading). This shifts every heading relative to the circular
    // mean (which is unaffected by the wrap boundary) before taking a linear median, then wraps
    // the result back into [0, 360).
    public static double getMedianHeading(ArrayList<Double> headings){
        if (headings.isEmpty()) {
            return 0;
        }

        double sumSin = 0;
        double sumCos = 0;
        for (double heading : headings) {
            double radians = Math.toRadians(heading);
            sumSin += Math.sin(radians);
            sumCos += Math.cos(radians);
        }
        double referenceHeading = Math.toDegrees(Math.atan2(sumSin, sumCos));

        ArrayList<Double> shiftedHeadings = new ArrayList<>();
        for (double heading : headings) {
            shiftedHeadings.add(CircleMath.boundAngleSignedDegrees(heading - referenceHeading));
        }

        double medianShift = getMedian(shiftedHeadings);
        return CircleMath.boundAngleDegrees(referenceHeading + medianShift + 360);
    }

    public static long getMedianTimestamp(ArrayList<Long> list){
        Collections.sort(list);

        int middle = list.size()/2;
        if(list.size() % 2 ==0){
            return (list.get(middle) + list.get(middle-1)) / 2;
        }else{
            return list.get(list.size()/2);
        }
    }

    // returns the input value bounded by the positive and negative threshold;
    public static double clamp(double value, double threshold) {
        return Math.max(Math.min(value, threshold), -threshold);
    }



}
