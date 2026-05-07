package us.dot.its.jpo.conflictmonitor.batch.client.spat;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.Spat;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatLog;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

@Slf4j
@Service
public class ProcessedSpatServiceImpl implements ProcessedSpatService {
    private final MongoTemplate mongoTemplate;

    @Autowired
    public ProcessedSpatServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public List<ProcessedSpat> listProcessedSpats(
            int intersectionId, Instant startTime, Instant endTime) {
        log.info("Finding spats intersection {}, startTime {}, endTime {}", intersectionId, startTime, endTime);
        Query query = new Query(
                Criteria.where("intersectionId").is(intersectionId)
                        .and("utcTimeStamp").gte(startTime).lte(endTime)
        );
        List<ProcessedSpat> spats = mongoTemplate.find(query, ProcessedSpat.class, "ProcessedSpat_MV");
        log.info("Found {} spats", spats.size());
        return spats;
    }

    @Override
    public SpatLog spatLogs(int intersectionId, Instant startTime, Instant endTime) {
        SpatLog spatLog = new SpatLog();
        spatLog.setIntersectionId(intersectionId);
        spatLog.setStartTime(startTime);
        spatLog.setEndTime(endTime);
        List<Spat> spats = listProcessedSpats(intersectionId, startTime, endTime)
                .stream()
                .map(Spat::fromProcessedSpat)
                .sorted(Comparator.comparing(Spat::getTimestamp))
                .toList();
        spatLog.setSpats(spats);
        return spatLog;
    }

    @Override
    public SignalGroupLog signalGroupLogs(int intersectionId, Instant startTime, Instant endTime) {
        SpatLog spatLog = spatLogs(intersectionId, startTime, endTime);
        return SignalGroupLog.fromSpatLog(spatLog);
    }
}
