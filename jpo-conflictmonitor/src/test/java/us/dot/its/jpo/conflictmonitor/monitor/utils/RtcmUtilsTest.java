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
import static org.hamcrest.Matchers.*;

@Slf4j
public class RtcmUtilsTest {



    @Test
    public void testCompare_Different() throws JsonProcessingException {
        ProcessedRTCM rtcm1 = getProcessedRTCM("processed-rtcm.json");
        ProcessedRTCM rtcm2 = getProcessedRTCM("processed-rtcm-different-value-same-msg-cnt.json");
        DiffResult<ProcessedRTCM> diffs = RtcmUtils.compare(rtcm1, rtcm2);
        log.debug(diffs.toString());
        var diffList = diffs.getDiffs();
        diffList.forEach(d -> log.info("{} differs", d.getFieldName()));
        assertThat(diffs.getNumberOfDiffs(), greaterThan(0));
    }

    @Test
    public void testCompare_Same() throws JsonProcessingException {
        ProcessedRTCM rtcm1 = getProcessedRTCM("processed-rtcm.json");
        ProcessedRTCM rtcm1copy = getProcessedRTCM("processed-rtcm-same-value-different-msg-cnt.json");
        DiffResult<ProcessedRTCM> diffs = RtcmUtils.compare(rtcm1, rtcm1copy);
        log.debug(diffs.toString());
        var diffList = diffs.getDiffs();
        diffList.forEach(d -> log.info("{} differs", d.getFieldName()));
        assertThat(diffs.getNumberOfDiffs(), equalTo(0));

    }

    private static final String RESOURCE_PATH = "/us/dot/its/jpo/conflictmonitor/monitor/utils/";


    private static ProcessedRTCM getProcessedRTCM(final String resourceName) throws JsonProcessingException {
        String spatStr = ResourceUtils.loadResource(RESOURCE_PATH + resourceName);
        ObjectMapper mapper = DateJsonMapper.getInstance();
        return mapper.readValue(spatStr, ProcessedRTCM.class);
    }
}
