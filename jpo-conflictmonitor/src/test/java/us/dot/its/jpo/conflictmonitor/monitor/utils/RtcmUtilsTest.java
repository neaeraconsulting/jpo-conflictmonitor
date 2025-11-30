package us.dot.its.jpo.conflictmonitor.monitor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.Diff;
import org.apache.commons.lang3.builder.DiffResult;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.testutils.ResourceUtils;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@Slf4j
public class RtcmUtilsTest {



    @Test
    public void testCompare_Different() throws JsonProcessingException {
        ProcessedRTCM rtcm1 = getProcessedRTCM("processed-rtcm1.json");
        ProcessedRTCM rtcm2 = getProcessedRTCM("processed-rtcm2.json");
        DiffResult<ProcessedRTCM> diffs = RtcmUtils.compare(rtcm1, rtcm2);
        log.info("diffs: {}", diffs);
        assertThat(diffs.getNumberOfDiffs(), greaterThan(0));
        var diffList = diffs.getDiffs();

    }

    private static final String RESOURCE_PATH = "/us/dot/its/jpo/conflictmonitor/monitor/utils/";


    private static ProcessedRTCM getProcessedRTCM(final String resourceName) throws JsonProcessingException {
        String spatStr = ResourceUtils.loadResource(RESOURCE_PATH + resourceName);
        ObjectMapper mapper = DateJsonMapper.getInstance();
        return mapper.readValue(spatStr, ProcessedRTCM.class);
    }
}
