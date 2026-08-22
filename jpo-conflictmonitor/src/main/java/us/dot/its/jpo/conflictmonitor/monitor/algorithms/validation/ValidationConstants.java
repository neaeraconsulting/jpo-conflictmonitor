package us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation;

public final class ValidationConstants {

    // Default algorithms implement Broadcast Rate standard from CTI-4501 v1
    public static final String DEFAULT_MAP_VALIDATION_ALGORITHM = "defaultMapValidationAlgorithm";
    public static final String DEFAULT_SPAT_VALIDATION_ALGORITHM = "defaultSpatValidationAlgorithm";
    public static final String DEFAULT_RTCM_VALIDATION_ALGORITHM = "defaultRtcmValidationAlgorithm";

    // CTI-4501 V2 and J3258 algorithms implement new Broadcast Rate standards
    public static final String CTI_4501_V2_MAP_VALIDATION_ALGORITHM = "cti4501V2MapValidationAlgorithm";
    public static final String CTI_4501_V2_SPAT_VALIDATION_ALGORITHM = "cti4501V2SpatValidationAlgorithm";
    public static final String J3258_RTCM_VALIDATION_ALGORITHM = "j3258RtcmValidationAlgorithm";

    public static final String ALTERNATE_MAP_VALIDATION_ALGORITHM = "alternateMapValidationAlgorithm";
    public static final String ALTERNATE_SPAT_VALIDATION_ALGORITHM = "alternateSpatValidationAlgorithm";

}
