package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinedRequestStatus {
    private SrmRequest srmRequest;
    private SsmStatus ssmStatus;
}
