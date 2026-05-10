package us.dot.its.jpo.conflictmonitor.batch.mongo;

import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * Service to create update ProcessedSpat to add a proper Mongo timestamps for utcTimeStamp
 * with an index on the timestamps.
 */
@Slf4j
@Service
public class ProcessedSpatCollectionUpdater {

    private static final String PROCESSED_SPAT = "ProcessedSpat";


    private final MongoTemplate mongoTemplate;

    @Autowired
    public ProcessedSpatCollectionUpdater(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void updateTimestamp() {
        log.info("Updating ProcessedSpat timestamps");
        // Add utc timestamp to ProcessedSpat
        MongoCollection<Document> collection = mongoTemplate.getCollection(PROCESSED_SPAT);

        Bson filter = new Document("utcTimeStampTS", new Document("$exists", false));

        List<Document> pipeline = Arrays.asList(
                new Document("$set", new Document("utcTimeStampTS",
                        new Document("$dateFromString",
                                new Document("dateString", "$utcTimeStamp")
                                        .append("onError", null)
                                        .append("onNull", null)
                        )
                ))
        );

        collection.updateMany(filter, pipeline);
        log.info("Finished updating ProcessedSpat timestamps");

        log.info("Updating ProcessedSpat indexes");
        // Create indexes
        var indexOps = mongoTemplate.indexOps(PROCESSED_SPAT);
        indexOps.createIndex(new Index().on("intersectionID", Sort.Direction.ASC));
        indexOps.createIndex(new Index().on("utcTimeStampTS", Sort.Direction.ASC));
        log.info("Finished updating ProcessedSpat indexes");

    }
}
