package us.dot.its.jpo.conflictmonitor.batch.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Covers ProcessedSpatCollectionUpdater#updateTimestamp: the ProcessedSpat backfill
 * update and index creation.
 */
@ExtendWith(MockitoExtension.class)
class ProcessedSpatCollectionUpdaterTest {

    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private MongoCollection<Document> collection;
    @Mock
    private IndexOperations indexOperations;

    @Test
    void updateTimestampRunsUpdateManyAgainstTheProcessedSpatCollection() {
        when(mongoTemplate.getCollection("ProcessedSpat")).thenReturn(collection);
        when(mongoTemplate.indexOps("ProcessedSpat")).thenReturn(indexOperations);

        var updater = new ProcessedSpatCollectionUpdater(mongoTemplate);
        updater.updateTimestamp();

        verify(collection).updateMany(any(Bson.class), anyList());
    }

    @Test
    void updateTimestampCreatesIndexesOnIntersectionIdAndTimestamp() {
        when(mongoTemplate.getCollection("ProcessedSpat")).thenReturn(collection);
        when(mongoTemplate.indexOps("ProcessedSpat")).thenReturn(indexOperations);

        var updater = new ProcessedSpatCollectionUpdater(mongoTemplate);
        updater.updateTimestamp();

        ArgumentCaptor<Index> indexCaptor = ArgumentCaptor.forClass(Index.class);
        verify(indexOperations, times(2)).createIndex(indexCaptor.capture());
        List<Document> indexKeys = indexCaptor.getAllValues().stream().map(Index::getIndexKeys).toList();
        assertThat(indexKeys, hasItem(hasKey("intersectionId")));
        assertThat(indexKeys, hasItem(hasKey("utcTimeStampTS")));
    }
}
