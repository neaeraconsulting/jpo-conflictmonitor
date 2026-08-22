package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.EvictingQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@JsonSerialize(using = TimestampBoundedQueue.SpatBoundedQueueSerializer.class)
@JsonDeserialize(using = TimestampBoundedQueue.SpatBoundedQueueDeserializer.class)
public class TimestampBoundedQueue {

    private final EvictingQueue<Long> queue;

    /**
     * Number of messages to measure the total duration of.
     */
    public static final int MAX_NUM_MESSAGES_FOR_DURATION = 10;

    /**
     * Max size of the queue is 1 more than the number of messages to measure
     * the duration of, since the duration of the final message ends with receipt
     * of the next message whose duration is not counted.
     */
    public static final int MAX_SIZE = MAX_NUM_MESSAGES_FOR_DURATION + 1;

    public TimestampBoundedQueue() {
        queue = EvictingQueue.create(MAX_SIZE);
    }

    @JsonCreator
    public TimestampBoundedQueue(Collection<Long> collection) {
        queue = EvictingQueue.create(MAX_SIZE);
        queue.addAll(collection);
    }

    public void add(long value) {
        queue.offer(value);
    }

    public int size() {
        return queue.size();
    }

    public int numberOfMessagesForDuration() {
        int size = queue.size();
        if (size == 0) return 0;
        return queue.size() - 1;
    }

    public List<Long> toList() {
        return new ArrayList<>(queue);
    }

    /**
     * Get last added timestamp
     * @return last added timestamp
     */
    public Optional<Long> latest() {
        final int size = size();
        if (size < 1)  return Optional.empty();
        List<Long> list = toList();
        return Optional.of(list.get(size - 1));
    }

    /**
     * Get the most recently added pair of timestamps
     * @return two timestamps or empty if there aren't two timestamps in the queue
     */
    public Optional<long[]> pair() {
        final int size = size();
        if (size < 2) {
            return Optional.empty();
        }
        List<Long> list = toList();
        long[] pair = new long[2];
        pair[0] = list.get(size - 2);
        pair[1] = list.get(size - 1);
        return Optional.of(pair);
    }

    public Optional<long[]> all() {
        if (size() < MAX_SIZE) {
            return Optional.empty();
        }
        return Optional.of(toList().stream().mapToLong(x -> x).toArray());
    }

    public static class SpatBoundedQueueSerializer extends JsonSerializer<TimestampBoundedQueue> {
        @Override
        public void serialize(TimestampBoundedQueue value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            if (value == null) return;
            List<Long> list = value.toList();
            serializers.defaultSerializeValue(list, gen);
        }
    }

    public static class SpatBoundedQueueDeserializer extends JsonDeserializer<TimestampBoundedQueue> {
        @Override
        public TimestampBoundedQueue deserialize(JsonParser parser, DeserializationContext context) throws IOException {
           var typeRef = new TypeReference<List<Long>>() {};
            List<Long> list = parser.readValueAs(typeRef);
            return new TimestampBoundedQueue(list);
        }
    }

}
