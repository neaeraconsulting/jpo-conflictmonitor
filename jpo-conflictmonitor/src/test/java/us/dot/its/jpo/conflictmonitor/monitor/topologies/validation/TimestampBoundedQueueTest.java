package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.junit.Test;

import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TimestampBoundedQueueTest {

    @Test
    public void testConstructor_defaultIsEmpty() {
        var queue = new TimestampBoundedQueue();
        assertThat(queue.size(), equalTo(0));
    }

    @Test
    public void testConstructor_fromCollectionUnderCapacity() {
        List<Long> values = List.of(1L, 2L, 3L);
        var queue = new TimestampBoundedQueue(values);
        assertThat(queue.size(), equalTo(3));
        assertThat(queue.toList(), equalTo(values));
    }

    @Test
    public void testConstructor_fromCollectionOverCapacity_evictsOldest() {
        List<Long> values = new ArrayList<>();
        for (long i = 1; i <= 13; i++) {
            values.add(i);
        }
        var queue = new TimestampBoundedQueue(values);
        assertThat(queue.size(), equalTo(TimestampBoundedQueue.MAX_SIZE));
        assertThat(queue.toList(), equalTo(List.of(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L)));
    }

    @Test
    public void testAdd_increasesSizeUntilCapacity() {
        var queue = new TimestampBoundedQueue();
        for (int i = 1; i <= TimestampBoundedQueue.MAX_SIZE; i++) {
            queue.add(i);
            assertThat(queue.size(), equalTo(i));
        }
    }

    @Test
    public void testAdd_evictsOldestWhenOverCapacity() {
        var queue = new TimestampBoundedQueue();
        for (long i = 1; i <= 12; i++) {
            queue.add(i);
        }
        assertThat(queue.size(), equalTo(TimestampBoundedQueue.MAX_SIZE));
        assertThat(queue.toList(), equalTo(List.of(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L)));
    }

    @Test
    public void testSize_emptyQueueIsZero() {
        assertThat(new TimestampBoundedQueue().size(), equalTo(0));
    }

    @Test
    public void testNumberOfMessagesForDuration_emptyQueueReturnsZero() {
        assertThat(new TimestampBoundedQueue().numberOfMessagesForDuration(), equalTo(0));
    }

    @Test
    public void testNumberOfMessagesForDuration_returnsSizeMinusOne() {
        var queue = new TimestampBoundedQueue();
        queue.add(1L);
        queue.add(2L);
        queue.add(3L);
        assertThat(queue.numberOfMessagesForDuration(), equalTo(queue.size() - 1));
    }

    @Test
    public void testNumberOfMessagesForDuration_capsAtTenWhenFull() {
        var queue = new TimestampBoundedQueue();
        for (long i = 1; i <= TimestampBoundedQueue.MAX_SIZE; i++) {
            queue.add(i);
        }
        assertThat(queue.numberOfMessagesForDuration(), equalTo(TimestampBoundedQueue.MAX_NUM_MESSAGES_FOR_DURATION));
    }

    @Test
    public void testToList_preservesInsertionOrder() {
        var queue = new TimestampBoundedQueue();
        queue.add(10L);
        queue.add(20L);
        queue.add(30L);
        assertThat(queue.toList(), equalTo(List.of(10L, 20L, 30L)));
    }

    @Test
    public void testToList_returnsIndependentCopy() {
        var queue = new TimestampBoundedQueue();
        queue.add(1L);
        var list = queue.toList();
        list.add(999L);
        assertThat(queue.toList(), equalTo(List.of(1L)));
    }

    @Test
    public void testPair_emptyWhenFewerThanTwoElements() {
        var queue = new TimestampBoundedQueue();
        assertThat(queue.pair(), equalTo(Optional.empty()));
        queue.add(1L);
        assertThat(queue.pair(), equalTo(Optional.empty()));
    }

    @Test
    public void testPair_presentWithExactlyTwoElements() {
        var queue = new TimestampBoundedQueue();
        queue.add(100L);
        queue.add(200L);
        var pairOpt = queue.pair();
        assertThat(pairOpt.isPresent(), equalTo(true));
        assertThat(pairOpt.get(), equalTo(new long[]{100L, 200L}));
    }

    @Test
    public void testPair_returnsMostRecentlyAddedPair() {
        var queue = new TimestampBoundedQueue();
        for (long i = 1; i <= 5; i++) {
            queue.add(i * 100L);
        }
        var pairOpt = queue.pair();
        assertThat(pairOpt.isPresent(), equalTo(true));
        assertThat(pairOpt.get(), equalTo(new long[]{400L, 500L}));
    }

    @Test
    public void testAll_emptyWhenFewerThanMaxSizeElements() {
        var queue = new TimestampBoundedQueue();
        for (long i = 1; i <= TimestampBoundedQueue.MAX_SIZE - 1; i++) {
            queue.add(i);
        }
        assertThat(queue.all(), equalTo(Optional.empty()));
    }

    @Test
    public void testAll_presentWithExactlyMaxSizeElements() {
        var queue = new TimestampBoundedQueue();
        long[] expected = new long[TimestampBoundedQueue.MAX_SIZE];
        for (long i = 1; i <= TimestampBoundedQueue.MAX_SIZE; i++) {
            queue.add(i);
            expected[(int) i - 1] = i;
        }
        var allOpt = queue.all();
        assertThat(allOpt.isPresent(), equalTo(true));
        assertThat(allOpt.get(), equalTo(expected));
    }

    @Test
    public void testAll_reflectsMostRecentElementsAfterExceedingMaxSize() {
        var queue = new TimestampBoundedQueue();
        for (long i = 1; i <= 15; i++) {
            queue.add(i);
        }
        var allOpt = queue.all();
        assertThat(allOpt.isPresent(), equalTo(true));
        assertThat(allOpt.get(), equalTo(new long[]{5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L}));
    }

    @Test
    public void testJsonSerializer_producesPlainTimestampArray() {
        var queue = new TimestampBoundedQueue();
        queue.add(1L);
        queue.add(2L);
        queue.add(3L);

        Serde<TimestampBoundedQueue> serde = JsonSerdes.TimestampBoundedQueue();
        byte[] bytes = serde.serializer().serialize("topic", queue);
        String json = new String(bytes, StandardCharsets.UTF_8);

        assertThat(json, equalTo("[1,2,3]"));
    }

    @Test
    public void testJsonSerde_roundTripReturnsEquivalentQueue() {
        var queue = new TimestampBoundedQueue();
        queue.add(1L);
        queue.add(2L);
        queue.add(3L);

        Serde<TimestampBoundedQueue> serde = JsonSerdes.TimestampBoundedQueue();
        byte[] bytes = serde.serializer().serialize("topic", queue);
        TimestampBoundedQueue result = serde.deserializer().deserialize("topic", bytes);

        assertThat(result.toList(), equalTo(queue.toList()));
    }

}
