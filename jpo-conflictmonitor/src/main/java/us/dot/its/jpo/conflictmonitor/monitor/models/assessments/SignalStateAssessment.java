package us.dot.its.jpo.conflictmonitor.monitor.models.assessments;

import lombok.Generated;
import lombok.Getter;
import lombok.Setter;


import java.util.List;

/*
 * This class has been deprecated and should no longer be used. Use StopLinePassageAssessment instead.
 */

@Getter
@Setter
@Generated
@Deprecated
public class SignalStateAssessment extends Assessment{

    /**
     * the time at when this Assessment was generated in utc milliseconds. This is deprecated in favor of the assessmentGeneratedAt in the parent class.
     * @deprecated
     */
    private long timestamp;

    /**
     * List of Signal State Assessment Groups that contribute to this assessment
     */
    private List<SignalStateAssessmentGroup> signalStateAssessmentGroup;

    public SignalStateAssessment(){
        super("SignalState");
    }
}
