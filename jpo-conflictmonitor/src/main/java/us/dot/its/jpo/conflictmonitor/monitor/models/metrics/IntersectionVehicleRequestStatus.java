package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;

public record IntersectionVehicleRequestStatus (IntersectionVehicleRequestKey requestKey, String status){}
