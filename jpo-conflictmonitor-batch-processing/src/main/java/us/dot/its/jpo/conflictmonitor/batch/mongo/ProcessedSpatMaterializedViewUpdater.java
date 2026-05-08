package us.dot.its.jpo.conflictmonitor.batch.mongo;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Service;

/**
 * Service to create a materialized view of the ProcessedSpat collection with proper Mongo timestamps,
 * with indexes on the timestamps.
 */
@Service
public class ProcessedSpatMaterializedViewUpdater {

    private static final String PROCESSED_SPAT = "ProcessedSpat";
    private static final String PROCESSED_SPAT_MV = "ProcessedSpat_MV";

    private final MongoTemplate mongoTemplate;

    @Autowired
    public  ProcessedSpatMaterializedViewUpdater(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void updateMaterializedView() {
        // Change utcTimeStamp from string to timestamp
        AggregationOperation setUtcTimeStamp = context -> new Document("$set",
                new Document("utcTimeStamp",
                        new Document("$dateFromString",
                                new Document("dateString", "$utcTimeStamp")
                                        .append("onError", null).append("onNull", null))));

        // Change odeReceivedAt from string to timestmamp
        AggregationOperation setOdeReceivedAt = context -> new Document("$set",
                new Document("odeReceivedAt",
                        new Document("$dateFromString",
                                new Document("dateString", "$odeReceivedAt")
                                        .append("onError", null).append("onNull", null))));

        // merge from ProcessedSpat into ProcessedSpat_MV
        AggregationOperation merge = context -> new Document("$merge",
                new Document("into", PROCESSED_SPAT_MV)
                        .append("whenMatched", "replace").append("whenNotMatched", "insert")
        );

        Aggregation aggregation = Aggregation.newAggregation(
                setUtcTimeStamp,
                setOdeReceivedAt,
                merge
        );

        // Aggregate creates or update ProcessedSpat_MV
        mongoTemplate.aggregate(aggregation, PROCESSED_SPAT, Document.class);

        // Create indexes
        var indexOps = mongoTemplate.indexOps(PROCESSED_SPAT_MV);
        indexOps.createIndex(new Index().on("intersectionID", Sort.Direction.ASC));
        indexOps.createIndex(new Index().on("utcTimeStamp", Sort.Direction.ASC));
        indexOps.createIndex(new Index().on("odeReceivedAt", Sort.Direction.ASC));
        indexOps.createIndex(new Index().on("recordGeneratedAt", Sort.Direction.ASC));

    }
}
