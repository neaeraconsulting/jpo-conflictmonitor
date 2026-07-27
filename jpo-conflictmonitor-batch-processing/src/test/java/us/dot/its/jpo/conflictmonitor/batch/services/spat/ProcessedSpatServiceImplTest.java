package us.dot.its.jpo.conflictmonitor.batch.services.spat;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupStateLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers ProcessedSpatServiceImpl: the Mongo query for ProcessedSpat, and the
 * spatLogs -> signalGroupLogs -> signalGroupIndicationLogs delegation chain.
 */
@ExtendWith(MockitoExtension.class)
class ProcessedSpatServiceImplTest {

    private static final Instant START = Instant.parse("2026-05-03T10:00:00Z");
    private static final Instant END = Instant.parse("2026-05-03T11:00:00Z");

    @Mock
    private MongoTemplate mongoTemplate;

    private ProcessedSpatServiceImpl service;

    private ProcessedSpat spat(Instant timestamp, ProcessedMovementState... states) {
        var spat = new ProcessedSpat();
        spat.setUtcTimeStampTS(timestamp);
        spat.setStates(List.of(states));
        return spat;
    }

    private ProcessedMovementState state() {
        var state = new ProcessedMovementState();
        state.setSignalGroup(1);
        var event = new ProcessedMovementEvent();
        event.setEventState(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED);
        state.setStateTimeSpeed(List.of(event));
        return state;
    }

    @BeforeEach
    void setUp() {
        service = new ProcessedSpatServiceImpl(mongoTemplate);
    }

    @Test
    void listProcessedSpatsQueriesByIntersectionIdAndTimeRange() {
        when(mongoTemplate.find(any(Query.class), eq(ProcessedSpat.class), eq("ProcessedSpat")))
                .thenReturn(List.of());

        service.listProcessedSpats(100, START, END);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).find(queryCaptor.capture(), eq(ProcessedSpat.class), eq("ProcessedSpat"));
        Document queryObject = queryCaptor.getValue().getQueryObject();
        assertThat(queryObject.get("intersectionId"), is(100));
        Document timeRange = (Document) queryObject.get("utcTimeStampTS");
        assertThat(timeRange.get("$gte"), is(START));
        assertThat(timeRange.get("$lte"), is(END));
    }

    @Test
    void spatLogsSortsSpatsByTimestampAscending() {
        ProcessedSpat later = spat(START.plusSeconds(30));
        ProcessedSpat earlier = spat(START.plusSeconds(10));
        when(mongoTemplate.find(any(Query.class), eq(ProcessedSpat.class), eq("ProcessedSpat")))
                .thenReturn(List.of(later, earlier));

        SpatLog result = service.spatLogs(100, START, END);

        assertThat(result.getIntersectionId(), is(100));
        assertThat(result.getStartTime(), is(START));
        assertThat(result.getEndTime(), is(END));
        assertThat(result.getSpats(), hasSize(2));
        assertThat(result.getSpats().getFirst().getTimestamp(), is(START.plusSeconds(10)));
        assertThat(result.getSpats().get(1).getTimestamp(), is(START.plusSeconds(30)));
    }

    @Test
    void signalGroupLogsDelegatesToSignalGroupStateLogFromSpatLog() {
        ProcessedSpat spat = spat(START.plusSeconds(10), state());
        when(mongoTemplate.find(any(Query.class), eq(ProcessedSpat.class), eq("ProcessedSpat")))
                .thenReturn(List.of(spat));

        SignalGroupStateLog result = service.signalGroupLogs(100, START, END);

        assertThat(result.getIntersectionId(), is(100));
        assertThat(result.getSignalGroupStates().get(1), hasSize(1));
        assertThat(result.getSignalGroupStates().get(1).getFirst().getEventState(),
                is(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED));
    }

    @Test
    void signalGroupIndicationLogsDelegatesToSignalGroupIndicationLogFromStateLog() {
        ProcessedSpat spat = spat(START.plusSeconds(10), state());
        when(mongoTemplate.find(any(Query.class), eq(ProcessedSpat.class), eq("ProcessedSpat")))
                .thenReturn(List.of(spat));

        SignalGroupIndicationLog result = service.signalGroupIndicationLogs(100, START, END);

        assertThat(result.getIndicationsMap().getIndications(1), hasSize(1));
        assertThat(result.getIndicationsMap().getIndications(1).getFirst().getIndication(), is(SpatSignalIndication.GREEN));
    }
}
