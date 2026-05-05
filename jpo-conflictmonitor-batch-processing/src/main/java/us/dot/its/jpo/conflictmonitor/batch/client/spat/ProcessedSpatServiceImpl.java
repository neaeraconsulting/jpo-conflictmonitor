package us.dot.its.jpo.conflictmonitor.batch.client.spat;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class ProcessedSpatServiceImpl implements ProcessedSpatService {
    private final MongoTemplate mongoTemplate;

    @Autowired
    public ProcessedSpatServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public List<ProcessedSpat> findByIntersectionIdAndTimestamp(
            int intersectionId, Instant startTime, Instant endTime) {
        log.info("Finding spats intersection {}, startTime {}, endTime {}", intersectionId, startTime, endTime);
        Query query = new Query(
                Criteria.where("intersectionId").is(intersectionId)
                        .and("utcTimeStamp").gte(Date.from(startTime)).lte(Date.from(endTime))
        );
        List<ProcessedSpat> spats = mongoTemplate.find(query, ProcessedSpat.class, "ProcessedSpat_MV");
        log.info("Found {} spats", spats.size());
        return spats;
    }
}
